from typing import Collection, Dict, Iterable, List, Optional

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.query_history import chunk_query_logs
from metaphor.common.utils import generate_querylog_id, md5_digest, start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Parsing,
    QueryLog,
    TypeEnum,
)
from metaphor.mssql.extractor import MssqlExtractor
from metaphor.mssql.mssql_client import MssqlClient
from metaphor.synapse.config import SynapseConfig
from metaphor.synapse.model import SynapseQueryLog
from metaphor.synapse.workspace_query import WorkspaceQuery

logger = get_logger()


class SynapseExtractor(MssqlExtractor):
    """Synapse metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SynapseExtractor":
        return SynapseExtractor(SynapseConfig.from_yaml_file(config_file))

    def __init__(self, config: SynapseConfig):
        super().__init__(
            config, "Synapse metadata crawler", Platform.SYNAPSE, DataPlatform.SYNAPSE
        )
        self._config = config
        self._filter = config.filter.normalize()
        self._lookback_days = config.query_log.lookback_days if config.query_log else 0

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(
            f"Fetching metadata from Synapse workspace: {self._config.server_name}"
        )

        sql_query_endpoint = f"{self._config.server_name}.sql.azuresynapse.net"
        sql_on_demand_query_endpoint = (
            f"{self._config.server_name}-ondemand.sql.azuresynapse.net"
        )
        serverless_client = MssqlClient(
            sql_on_demand_query_endpoint, self._config.username, self._config.password
        )
        dedicated_client = MssqlClient(
            sql_query_endpoint, self._config.username, self._config.password
        )
        start_date = start_of_day(self._lookback_days)
        end_date = start_of_day()
        entities: List[ENTITY_TYPES] = []
        querylog_list: List[QueryLog] = []

        # Serverless sqlpool
        try:
            for database in serverless_client.get_databases():
                if not self._filter.include_database(database.name):
                    continue
                tables = serverless_client.get_tables(database.name)
                datasets = self._map_tables_to_dataset(
                    self._config.server_name, database, tables
                )
                self._set_foreign_keys_to_dataset(
                    datasets, database.name, serverless_client
                )
                entities.extend(datasets.values())
            if self._lookback_days > 0:
                querlogs = self._map_query_log(
                    WorkspaceQuery.get_sql_pool_query_logs(
                        serverless_client.config,
                        start_date,
                        end_date,
                    )
                )
                querylog_list.extend(querlogs)
        except Exception as error:
            logger.exception(f"serverless sqlpool error: {error}")

        # Dedicated sqlpool
        try:
            for database in dedicated_client.get_databases():
                if not self._filter.include_database(database.name):
                    continue
                tables = dedicated_client.get_tables(database.name)
                datasets = self._map_tables_to_dataset(
                    self._config.server_name, database, tables
                )
                self._set_foreign_keys_to_dataset(
                    datasets, database.name, dedicated_client
                )
                entities.extend(datasets.values())
                if self._lookback_days > 0:
                    querlogs = self._map_query_log(
                        WorkspaceQuery.get_dedicated_sql_pool_query_logs(
                            dedicated_client.config,
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

    def _map_query_log(
        self, rows: Iterable[SynapseQueryLog], database: Optional[str] = None
    ):
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
            queryLog.type = (
                self._map_query_type(row.query_operation)
                if row.query_operation
                else None
            )
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

    _query_type_map = {
        "CREATE": TypeEnum.CREATE_TABLE,
        "SELECT": TypeEnum.SELECT,
        "UPDATE": TypeEnum.UPDATE,
        "DROP": TypeEnum.DROP_TABLE,
        "ALTER": TypeEnum.ALTER_TABLE,
        "TRUNCATE": TypeEnum.TRUNCATE,
        "INSERT": TypeEnum.INSERT,
        "DELETE": TypeEnum.DELETE,
    }

    @staticmethod
    def _map_query_type(operation: str) -> TypeEnum:
        return SynapseExtractor._query_type_map.get(operation.upper(), TypeEnum.OTHER)
