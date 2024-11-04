import json
from dataclasses import asdict, dataclass
from typing import Collection, Dict, List, Optional, Type

from requests.auth import HTTPBasicAuth

from metaphor.common.api_request import ApiError, make_request
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    to_dataset_entity_id_from_logical_id,
    to_pipeline_entity_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.common.utils import dedup_lists, safe_float
from metaphor.fivetran.config import FivetranRunConfig
from metaphor.fivetran.models import (
    ConnectorPayload,
    DataT,
    DestinationPayload,
    GenericListResponse,
    GenericResponse,
    GroupPayload,
    MetadataColumnPayload,
    MetadataSchemaPayload,
    MetadataTablePayload,
    SourceMetadataPayload,
    UserPayload,
)
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    EntityUpstream,
    FieldMapping,
    FiveTranConnectorStatus,
    FivetranPipeline,
    Pipeline,
    PipelineInfo,
    PipelineLogicalID,
    PipelineMapping,
    PipelineType,
    SourceField,
)

logger = get_logger()

PLATFORM_MAPPING = {
    "azure_sql_data_warehouse": DataPlatform.SYNAPSE,
    "big_query": DataPlatform.BIGQUERY,
    "mysql_warehouse": DataPlatform.MYSQL,
    "mysql_rds_warehouse": DataPlatform.MYSQL,
    "aurora_warehouse": DataPlatform.MYSQL,
    "postgres_warehouse": DataPlatform.POSTGRESQL,
    "postgres_rds_warehouse": DataPlatform.POSTGRESQL,
    "aurora_postgres_warehouse": DataPlatform.POSTGRESQL,
    "postgres_gcp_warehouse": DataPlatform.POSTGRESQL,
    "redshift": DataPlatform.REDSHIFT,
    "snowflake": DataPlatform.SNOWFLAKE,
}

SOURCE_PLATFORM_MAPPING = {
    "documentdb": DataPlatform.DOCUMENTDB,
    "aurora": DataPlatform.MYSQL,
    "aurora_postgres": DataPlatform.POSTGRESQL,
    "maria_azure": DataPlatform.MYSQL,
    "mysql_azure": DataPlatform.MYSQL,
    "azure_postgres": DataPlatform.POSTGRESQL,
    "azure_sql_db": DataPlatform.MSSQL,
    "google_cloud_mysql": DataPlatform.MYSQL,
    "google_cloud_postgresql": DataPlatform.POSTGRESQL,
    "google_cloud_sqlserver": DataPlatform.MSSQL,
    "heroku_postgres": DataPlatform.POSTGRESQL,
    "sql_server_hva": DataPlatform.MSSQL,
    "magento_mysql": DataPlatform.MYSQL,
    "magento_mysql_rds": DataPlatform.MYSQL,
    "maria": DataPlatform.MYSQL,
    "maria_rds": DataPlatform.MYSQL,
    "mysql": DataPlatform.MYSQL,
    "mysql_rds": DataPlatform.MYSQL,
    "postgres": DataPlatform.POSTGRESQL,
    "postgres_rds": DataPlatform.POSTGRESQL,
    "sql_server": DataPlatform.MSSQL,
    "sql_server_rds": DataPlatform.MSSQL,
    "snowflake_db": DataPlatform.SNOWFLAKE,
}


@dataclass
class ColumnMetadata:
    name_in_source: str
    name_in_destination: str
    type_in_source: Optional[str]
    type_in_destination: Optional[str]
    is_primary_key: bool
    is_foreign_key: bool


@dataclass
class TableMetadata:
    name_in_source: str
    name_in_destination: str
    columns: List[ColumnMetadata]


@dataclass
class SchemaMetadata:
    # name_in_source could be null
    name_in_source: Optional[str]

    name_in_destination: str
    tables: List[TableMetadata]


class FivetranExtractor(BaseExtractor):
    """Fivetran metadata extractor"""

    _description = "Fivetran metadata crawler"
    _platform = Platform.FIVETRAN

    @staticmethod
    def from_config_file(config_file: str) -> "FivetranExtractor":
        return FivetranExtractor(FivetranRunConfig.from_yaml_file(config_file))

    def __init__(self, config: FivetranRunConfig) -> None:
        super().__init__(config)
        self._auth = HTTPBasicAuth(username=config.api_key, password=config.api_secret)
        self._datasets: Dict[str, Dataset] = {}
        self._source_datasets: Dict[str, Dataset] = {}
        self._pipelines: Dict[str, Pipeline] = {}
        self._destinations: Dict[str, DestinationPayload] = {}
        self._source_metadata: Dict[str, SourceMetadataPayload] = {}
        self._users: Dict[str, str] = {}
        self._base_url = "https://api.fivetran.com/v1"
        self._requests_timeout = config.requests.timeout

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Fivetran")

        for source_metadata in self.get_all_source_metadata():
            self._source_metadata[source_metadata.id] = source_metadata

        self.get_destinations()
        connectors = self.get_connectors()
        self.get_users()

        for connector in connectors:
            schemas = self.get_metadata_schemas(connector.id)
            tables = self.get_metadata_tables(connector.id)
            columns = self.get_metadata_columns(connector.id)

            connector_schema_metadata = self.process_metadata(schemas, tables, columns)

            self.map_to_datasets(connector, connector_schema_metadata)

        return [
            *self._datasets.values(),
            *self._source_datasets.values(),
            *self._pipelines.values(),
        ]

    @staticmethod
    def process_metadata(
        schemas: List[MetadataSchemaPayload],
        tables: List[MetadataTablePayload],
        columns: List[MetadataColumnPayload],
    ) -> List[SchemaMetadata]:
        metadata = []

        schema_map = {}
        for schema in schemas:
            schema_data = SchemaMetadata(
                name_in_source=schema.name_in_source,
                name_in_destination=schema.name_in_destination,
                tables=[],
            )
            schema_map[schema.id] = schema_data
            metadata.append(schema_data)

        table_map = {}
        for table in tables:
            parent_schema: Optional[SchemaMetadata] = schema_map.get(table.parent_id)
            if parent_schema is None:
                continue

            table_data = TableMetadata(
                name_in_source=table.name_in_source,
                name_in_destination=table.name_in_destination,
                columns=[],
            )

            parent_schema.tables.append(table_data)
            table_map[table.id] = table_data

        for column in columns:
            parent_table: Optional[TableMetadata] = table_map.get(column.parent_id)
            if parent_table is None:
                continue

            column_data = ColumnMetadata(
                name_in_source=column.name_in_source,
                name_in_destination=column.name_in_destination,
                type_in_source=column.type_in_source,
                type_in_destination=column.type_in_destination,
                is_primary_key=column.is_primary_key,
                is_foreign_key=column.is_foreign_key,
            )
            parent_table.columns.append(column_data)

        return metadata

    def map_to_datasets(
        self, connector: ConnectorPayload, schemas: List[SchemaMetadata]
    ) -> None:
        destination = self._destinations.get(connector.group_id)
        assert destination, "destination for connector should exist"

        serialized_schema_metadata = json.dumps([asdict(s) for s in schemas])

        self._init_pipeline(connector, serialized_schema_metadata)

        for schema in schemas:
            for table in schema.tables:
                self._init_dataset(
                    destination=destination,
                    schema=schema,
                    table=table,
                    connector=connector,
                )

    def _get_source_logical_id(
        self,
        source_db,
        schema: SchemaMetadata,
        table: TableMetadata,
        connector: ConnectorPayload,
    ) -> DatasetLogicalID:
        source_dataset_name = dataset_normalized_name(
            source_db, schema.name_in_source, table.name_in_source
        )
        source_platform = (
            SOURCE_PLATFORM_MAPPING.get(connector.service) or DataPlatform.EXTERNAL
        )

        source_logical_id = DatasetLogicalID(
            name=source_dataset_name,
            platform=source_platform,
            account=self.get_source_account_from_config(
                connector.config, source_platform
            ),
        )

        return source_logical_id

    def _init_dataset(
        self,
        schema: SchemaMetadata,
        table: TableMetadata,
        destination: DestinationPayload,
        connector: ConnectorPayload,
    ):
        db = self.get_database_name_from_destination(destination)
        destination_dataset_name = dataset_normalized_name(
            db, schema.name_in_destination, table.name_in_destination
        )
        destination_platform = PLATFORM_MAPPING.get(destination.service)

        pipeline_entity_id = str(
            to_pipeline_entity_id(connector.id, PipelineType.FIVETRAN)
        )

        pipeline_mapping = PipelineMapping(
            is_virtual=True,
            source_entity_id=None,
            pipeline_entity_id=pipeline_entity_id,
        )

        dataset = Dataset(
            logical_id=DatasetLogicalID(
                name=destination_dataset_name,
                platform=destination_platform,
                account=self.get_snowflake_account_from_config(destination.config),
            ),
            entity_upstream=EntityUpstream(
                source_entities=[],
                field_mappings=[],
            ),
        )
        dataset_id = str(to_dataset_entity_id_from_logical_id(dataset.logical_id))

        pipeline = self._pipelines[connector.id]
        assert pipeline.fivetran is not None
        pipeline.fivetran.targets = dedup_lists(pipeline.fivetran.targets, [dataset_id])

        if connector.service in SOURCE_PLATFORM_MAPPING:
            source_db = self.get_database_name_from_connector(connector)
            source_logical_id = self._get_source_logical_id(
                source_db, schema, table, connector
            )

            source_entity_id = str(
                to_dataset_entity_id_from_logical_id(source_logical_id)
            )

            self._source_datasets[source_entity_id] = Dataset(
                logical_id=source_logical_id
            )

            # Non virtual upstream entity
            pipeline_mapping.is_virtual = False
            pipeline_mapping.source_entity_id = source_entity_id

            dataset.entity_upstream.source_entities = [source_entity_id]

            pipeline.fivetran.sources = list(
                set(pipeline.fivetran.sources + [source_entity_id])
            )

            for column in table.columns:
                field_mapping = FieldMapping(
                    destination=column.name_in_destination, sources=[]
                )
                field_mapping.sources.append(
                    SourceField(
                        dataset=source_logical_id,
                        source_entity_id=source_entity_id,
                        field=column.name_in_source,
                    )
                )
                dataset.entity_upstream.field_mappings.append(field_mapping)

        dataset.pipeline_info = PipelineInfo(pipeline_mapping=[pipeline_mapping])

        pipeline.fivetran.sources.sort()
        pipeline.fivetran.targets.sort()

        self._datasets[destination_dataset_name] = dataset

    def _init_pipeline(
        self,
        connector: ConnectorPayload,
        serialized_schema_metadata: str,
    ) -> Pipeline:
        pipeline = self._create_fivetran_pipeline(
            connector,
            self._source_metadata.get(connector.service),
            serialized_schema_metadata,
            self._users.get(connector.connected_by) if connector.connected_by else None,
        )

        self._pipelines[connector.id] = pipeline
        return pipeline

    @staticmethod
    def _create_fivetran_pipeline(
        connector: ConnectorPayload,
        source_metadata: Optional[SourceMetadataPayload],
        serialized_schema_metadata: str,
        creator_email: Optional[str],
    ) -> Pipeline:
        url = f"https://fivetran.com/dashboard/connectors/{connector.id}"

        connector_type_name = source_metadata.name if source_metadata else None
        icon_url = source_metadata.icon_url if source_metadata else None
        sync_interval = safe_float(connector.sync_frequency)

        fivetran = FivetranPipeline(
            status=FiveTranConnectorStatus(
                setup_state=connector.status.setup_state,
                update_state=connector.status.update_state,
                sync_state=connector.status.sync_state,
            ),
            config=json.dumps(connector.config),
            created=connector.created_at,
            last_succeeded=connector.succeeded_at,
            paused=connector.paused,
            connector_url=url,
            connector_name=connector.schema_,
            connector_type_name=connector_type_name,
            connector_type_id=connector.service,
            connector_logs_url=f"{url}/logs",
            schema_metadata=serialized_schema_metadata,
            sync_interval_in_minute=sync_interval,
            creator_email=creator_email,
            icon_url=icon_url,
            sources=[],
            targets=[],
        )

        return Pipeline(
            logical_id=PipelineLogicalID(name=connector.id, type=PipelineType.FIVETRAN),
            fivetran=fivetran,
        )

    @staticmethod
    def get_snowflake_account_from_config(config: dict) -> Optional[str]:
        host = config.get("host")

        if host is None:
            return None

        return normalize_snowflake_account(host)

    @staticmethod
    def get_source_account_from_config(
        config: dict, source_platform: DataPlatform
    ) -> Optional[str]:
        if source_platform == DataPlatform.SNOWFLAKE:
            return FivetranExtractor.get_snowflake_account_from_config(config)
        elif source_platform == DataPlatform.MSSQL:
            host = config.get("host")
            if host:
                return host.lower()
        return None

    @staticmethod
    def get_database_name_from_destination(
        destination: DestinationPayload,
    ) -> Optional[str]:
        if destination.service == "big_query":
            return destination.config.get("project_id")
        return destination.config.get("database")

    @staticmethod
    def get_database_name_from_connector(connector: ConnectorPayload) -> Optional[str]:
        config = connector.config
        return config.get("database") if isinstance(config, dict) else None

    def get_group_ids(self) -> List[str]:
        groups: List[GroupPayload] = self._get_all(
            url=f"{self._base_url}/groups", type_=GroupPayload
        )
        return [group.id for group in groups]

    def get_destinations(self) -> None:
        groups = self.get_group_ids()
        for group in groups:
            try:
                destination = self.get_destination_info(group)
            except ApiError as error:
                logger.error(error)
                continue

            if destination is None:
                continue

            if destination.service not in PLATFORM_MAPPING:
                logger.warning(f"Not supported destination: {destination.service}")
                continue

            self._destinations[destination.group_id] = destination

    def get_connectors(self) -> List[ConnectorPayload]:
        connectors = []

        for destination in self._destinations.values():
            for connector in self.get_connectors_in_group(destination.group_id):
                try:
                    connector_detail = self.get_connector_detail(connector.id)
                except ApiError as error:
                    logger.error(error)
                    continue

                if connector_detail is None:
                    continue
                connectors.append(connector_detail)

        return connectors

    def get_users(self) -> None:
        for destination in self._destinations.values():
            for user in self.get_users_in_group(destination.group_id):
                self._users[user.id] = user.email

    def get_destination_info(self, group_id: str) -> Optional[DestinationPayload]:
        response: GenericResponse[DestinationPayload] = self._call_get(
            url=f"{self._base_url}/destinations/{group_id}",
            type_=GenericResponse[DestinationPayload],
        )
        return response.data

    def get_connector_detail(self, connector_id: str) -> Optional[ConnectorPayload]:
        response: GenericResponse[ConnectorPayload] = self._call_get(
            url=f"{self._base_url}/connectors/{connector_id}",
            type_=GenericResponse[ConnectorPayload],
        )
        return response.data

    def get_connectors_in_group(self, group_id: str) -> List[ConnectorPayload]:
        return self._get_all(
            url=f"{self._base_url}/groups/{group_id}/connectors", type_=ConnectorPayload
        )

    def get_users_in_group(self, group_id: str) -> List[UserPayload]:
        return self._get_all(
            url=f"{self._base_url}/groups/{group_id}/users", type_=UserPayload
        )

    def get_metadata_schemas(self, connector_id: str):
        return self._get_all(
            url=f"{self._base_url}/metadata/connectors/{connector_id}/schemas",
            type_=MetadataSchemaPayload,
        )

    def get_metadata_tables(self, connector_id: str):
        return self._get_all(
            url=f"{self._base_url}/metadata/connectors/{connector_id}/tables",
            type_=MetadataTablePayload,
        )

    def get_metadata_columns(self, connector_id: str):
        return self._get_all(
            url=f"{self._base_url}/metadata/connectors/{connector_id}/columns",
            type_=MetadataColumnPayload,
        )

    def get_all_source_metadata(self):
        return self._get_all(
            url=f"{self._base_url}/metadata/connectors",
            type_=SourceMetadataPayload,
        )

    def _get_all(self, url: str, type_: Type[DataT]) -> List[DataT]:
        result: List[DataT] = []
        next_cursor = None

        while True:
            query = {"cursor": next_cursor, "limit": "1000"}

            target_type: Type = GenericListResponse[type_]  # type: ignore
            try:
                response = self._call_get(
                    url=url,
                    type_=target_type,
                    params=query,
                )
            except ApiError as error:
                if error.status_code == 401:
                    raise error
                logger.error(error)
                return result

            result.extend(response.data.items)
            next_cursor = response.data.next_cursor
            if next_cursor is None:
                return result

    def _call_get(self, url: str, **kwargs):
        headers = {"Accept": "application/json;version=2"}
        return make_request(
            url=url,
            headers=headers,
            timeout=self._requests_timeout,
            auth=self._auth,
            **kwargs,
        )
