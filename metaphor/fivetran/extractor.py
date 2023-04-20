from typing import Collection, Dict, List, Optional, Tuple, Type

from requests.auth import HTTPBasicAuth

from metaphor.common.api_request import get_request
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
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
)
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUpstream,
    EntityType,
    FieldMapping,
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


class FivetranExtractor(BaseExtractor):
    """Fivetran metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "FivetranExtractor":
        return FivetranExtractor(FivetranRunConfig.from_yaml_file(config_file))

    def __init__(self, config: FivetranRunConfig) -> None:
        super().__init__(config, "Fivetran metadata crawler", Platform.GLUE)

        self._auth = HTTPBasicAuth(username=config.api_key, password=config.api_secret)
        self._destinations: Dict[str, DestinationPayload] = {}
        self._datasets: Dict[str, Dataset] = {}
        self._base_url = "https://api.fivetran.com/v1"

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Fivetran")

        self.get_destinations()
        connectors = self.get_connectors()

        for connector in connectors:
            schemas = self.get_metadata_schemas(connector.id)
            tables = self.get_metadata_tables(connector.id)
            columns = self.get_metadata_columns(connector.id)

            self.map_to_datasets(connector, schemas, tables, columns)

        return self._datasets.values()

    def map_to_datasets(
        self,
        connector: ConnectorPayload,
        schemas: List[MetadataSchemaPayload],
        tables: List[MetadataTablePayload],
        columns: List[MetadataColumnPayload],
    ):
        destination = self._destinations.get(connector.group_id)
        assert destination, "destination for connector should exist"

        source_db, destination_db = self.get_database_name_from_connector(connector)
        schema_dict: Dict[str, MetadataSchemaPayload] = {}
        for item in schemas:
            schema_dict[item.id] = item

        column_dict: Dict[str, List[MetadataColumnPayload]] = {}
        for column in columns:
            column_dict.setdefault(column.parent_id, []).append(column)

        for table in tables:
            schema: Optional[MetadataSchemaPayload] = schema_dict.get(table.parent_id)
            if schema is None:
                continue

            source_dataset_name = dataset_normalized_name(
                source_db, schema.name_in_destination, table.name_in_destination
            )
            source_platform = (
                SOURCE_PLATFORM_MAPPING.get(connector.service) or DataPlatform.EXTERNAL
            )
            source_account = (
                self.get_snowflake_account_from_config(connector.config)
                if source_platform == DataPlatform.SNOWFLAKE
                else connector.id
                if source_platform == DataPlatform.EXTERNAL
                else None
            )

            source_logical_id = DatasetLogicalID(
                name=dataset_normalized_name(table=source_dataset_name),
                platform=source_platform,
                account=source_account,
            )
            source_entity_id = str(
                to_dataset_entity_id_from_logical_id(source_logical_id)
            )

            destination_dataset_name = dataset_normalized_name(
                destination_db, schema.name_in_source, table.name_in_source
            )
            destination_platform = PLATFORM_MAPPING.get(destination.service)
            destination_account = self.get_snowflake_account_from_config(
                destination.config
            )

            dataset = Dataset(
                entity_type=EntityType.DATASET,
                logical_id=DatasetLogicalID(
                    name=destination_dataset_name,
                    platform=destination_platform,
                    account=destination_account,
                ),
            )

            dataset.upstream = DatasetUpstream(
                source_datasets=[source_entity_id], field_mappings=[]
            )
            for column in column_dict.get(table.id, []):
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
                dataset.upstream.field_mappings.append(field_mapping)

            self._datasets[destination_dataset_name] = dataset

    def get_snowflake_account_from_config(self, config: dict) -> Optional[str]:
        host = config.get("host")

        if host is None:
            return None

        # remove snowflakecomputing.com parts
        return ".".join(host.split(".")[:-2])

    def get_database_name_from_connector(
        self, connector: ConnectorPayload
    ) -> Tuple[Optional[str], Optional[str]]:
        def get_database_name_from_destination():
            destination = self._destinations.get(connector.group_id)
            if destination is None or not isinstance(destination.config, dict):
                return None

            if destination.service == "big_query":
                return destination.config.get("project_id")
            return destination.config.get("database")

        config = connector.config
        source_db = config.get("database") if isinstance(config, dict) else None

        return source_db, get_database_name_from_destination()

    def get_group_ids(self) -> List[str]:
        groups: List[GroupPayload] = self._get_all(
            url=f"{self._base_url}/groups", type_=GroupPayload
        )
        return [group.id for group in groups]

    def get_destinations(self):
        groups = self.get_group_ids()
        for group in groups:
            destination = self.get_destination_info(group)
            if destination is None:
                continue

            if destination.service not in PLATFORM_MAPPING:
                logger.warn(f"Not supported destination: {destination.service}")
                continue

            self._destinations[destination.group_id] = destination

    def get_connectors(self) -> List[ConnectorPayload]:
        connectors = []

        for destination in self._destinations.values():
            for connector in self.get_connector_in_group(destination.group_id):
                connector_detail = self.get_connector_detail(connector.id)
                if connector_detail is None:
                    continue
                connectors.append(connector_detail)

        return connectors

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

    def get_connector_in_group(self, group_id: str) -> List[ConnectorPayload]:
        return self._get_all(
            url=f"{self._base_url}/groups/{group_id}/connectors", type_=ConnectorPayload
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

    def _get_all(self, url: str, type_: Type[DataT]) -> List[DataT]:
        result = []
        next_cursor = None

        while True:
            query = {"cursor": next_cursor, "limit": "1000"}

            target_type: Type = GenericListResponse[type_]  # type: ignore
            response = self._call_get(
                url=url,
                type_=target_type,
                params=query,
            )
            result.extend(response.data.items)
            next_cursor = response.data.next_cursor
            if next_cursor is None:
                return result

    def _call_get(self, url: str, **kwargs):
        headers = {"Accept": "application/json;version=2"}
        return get_request(url=url, headers=headers, auth=self._auth, **kwargs)
