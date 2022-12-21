import json
from typing import Collection, Dict, Iterable, List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.query_history import chunk_query_logs
from metaphor.common.utils import generate_querylog_id, md5_digest, start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    CustomMetadata,
    CustomMetadataItem,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    EntityType,
    MaterializationType,
    Parsing,
    QueryLog,
    SchemaField,
    SQLSchema,
    TypeEnum,
)
from metaphor.synapse.config import SynapseConfig
from metaphor.synapse.model import SynapseDatabase, SynapseQueryLog, SynapseTable
from metaphor.synapse.workspace_client import WorkspaceClient

logger = get_logger()


class SynapseExtractor(BaseExtractor):
    """Synapse metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SynapseExtractor":
        return SynapseExtractor(SynapseConfig.from_yaml_file(config_file))

    def __init__(self, config: SynapseConfig):
        super().__init__(config, "Synapse metadata crawler", Platform.SYNAPSE)
        self._client = WorkspaceClient(
            config.workspace_name, config.username, config.password
        )
        self._tenant_id = config.tenant_id
        self._lookback_days = config.query_log.lookback_days if config.query_log else 0

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(
            f"Fetching metadata from Synapse workspace: {self._client.workspace_name}"
        )

        start_date = start_of_day(self._lookback_days)
        end_date = start_of_day()
        entities: List[ENTITY_TYPES] = []
        querylog_list: List[QueryLog] = []

        # Serverless sqlpool
        try:
            for database in self._client.get_databases():
                tables = self._client.get_tables(database.name)
                datasets = self._map_tables_to_dataset(
                    self._client.workspace_name, database, tables
                )
                entities.extend(datasets)
            if self._lookback_days > 0:
                querlogs = self._map_query_log(
                    self._client.get_sql_pool_query_logs(
                        start_date,
                        end_date,
                    )
                )
                querylog_list.extend(querlogs)
        except Exception as error:
            logger.exception(f"serverless sqlpool error: {error}")

        # Dedicated sqlpool
        try:
            for database in self._client.get_databases(is_dedicated=True):
                tables = self._client.get_tables(database.name, is_dedicated=True)
                datasets = self._map_tables_to_dataset(
                    self._client.workspace_name, database, tables
                )
                entities.extend(datasets)
                if self._lookback_days > 0:
                    querlogs = self._map_query_log(
                        self._client.get_dedicated_sql_pool_query_logs(
                            database.name,
                            start_date,
                            end_date,
                        ),
                        database.name,
                    )
                    querylog_list.extend(querlogs)
        except Exception as error:
            logger.exception(f"dedicated sqlpool error: {error}")

        entities.extend(chunk_query_logs(querylog_list))
        return entities

    def _map_tables_to_dataset(
        self,
        workspace_name: str,
        database: SynapseDatabase,
        tables: List[SynapseTable],
    ):
        dataset_map: Dict[str, Dataset] = {}
        for table in tables:
            dataset = Dataset()
            dataset.created_at = table.create_time
            dataset.entity_type = EntityType.DATASET
            dataset.logical_id = DatasetLogicalID(
                account=workspace_name,
                name=dataset_normalized_name(
                    db=database.name, schema=table.schema_name, table=table.name
                ),
                platform=DataPlatform.SYNAPSE,
            )
            dataset.structure = DatasetStructure(
                database=database.name,
                schema=table.schema_name,
                table=table.name,
            )
            fields = []
            primary_keys = []
            foreign_keys = []
            for column in table.column_dict.values():
                fields.append(
                    SchemaField(
                        subfields=None,
                        field_name=column.name,
                        field_path=column.name,
                        max_length=column.max_length if column.max_length > 0 else None,
                        nullable=column.is_nullable,
                        precision=column.precision,
                        native_type=column.type,
                        is_unique=column.is_unique,
                    )
                )
                if column.is_primary_key:
                    primary_keys.append(column.name)
                if column.is_foreign_key:
                    foreign_keys.append(column.name)

            dataset.custom_metadata = CustomMetadata(metadata=self._get_metadata(table))

            dataset.schema = DatasetSchema(
                sql_schema=SQLSchema(
                    table_schema=table.schema_name,
                    primary_key=primary_keys,
                    foreign_key=foreign_keys,
                ),
                fields=fields,
            )

            if table.is_external:
                dataset.schema.sql_schema.materialization = MaterializationType.EXTERNAL
            elif table.type == "V":
                dataset.schema.sql_schema.materialization = MaterializationType.VIEW
            else:
                dataset.schema.sql_schema.materialization = MaterializationType.TABLE

            dataset_map[table.id] = dataset

        return dataset_map.values()

    def _get_metadata(self, table: SynapseTable) -> List[CustomMetadataItem]:
        items: List[CustomMetadataItem] = []

        items.append(CustomMetadataItem("tenant_id", json.dumps(self._tenant_id)))

        if table.external_file_format:
            items.append(
                CustomMetadataItem("format", json.dumps(table.external_file_format))
            )

        if table.external_source:
            items.append(
                CustomMetadataItem("source", json.dumps(table.external_source))
            )
        return items

    def _map_query_log(self, rows: Iterable[SynapseQueryLog], database: str = None):
        querylog_map: Dict[str, QueryLog] = {}
        for row in rows:
            query_id = (
                f"{row.request_id}:{row.session_id}"
                if row.session_id
                else row.request_id
            )

            if query_id in querylog_map:
                continue

            queryLog = QueryLog()
            query_id = generate_querylog_id(DataPlatform.SYNAPSE.name, row.request_id)
            queryLog.id = query_id
            queryLog.query_id = row.request_id
            queryLog.type = self._map_query_type(row.query_operation)
            queryLog.platform = DataPlatform.SYNAPSE
            queryLog.start_time = row.start_time
            queryLog.duration = row.duration / 1000.0
            queryLog.user_id = row.login_name
            queryLog.sql = row.sql_query
            queryLog.sql_hash = md5_digest(row.sql_query.encode("utf-8"))
            if row.row_count:
                queryLog.rows_read = float(row.row_count)
            if row.query_size:
                queryLog.bytes_read = float(row.query_size * 1024)
            if row.error:
                queryLog.parsing = Parsing(error_message=row.error)
            queryLog.default_database = database
            querylog_map[query_id] = queryLog
        return querylog_map.values()

    def _map_query_type(self, operation: str) -> TypeEnum:
        operation = operation.upper()
        if operation in ["CREATE", "DROP", "ALTER", "TRUNCATE"]:
            return TypeEnum.DDL
        if operation in ["INSERT", "UPATE", "DELETE", "CALL", "EXPALIN CALL", "LOCK"]:
            return TypeEnum.DML
        else:
            return None
