from typing import Collection, Dict, Iterable, Iterator, List, Optional, Set

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import generate_querylog_id, md5_digest, start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    LogType,
    Parsing,
    QueryLog,
)
from metaphor.mssql.extractor import MssqlExtractor
from metaphor.mssql.mssql_client import MssqlClient
from metaphor.synapse.config import SynapseConfig
from metaphor.synapse.model import SynapseQueryLog
from metaphor.synapse.workspace_query import WorkspaceQuery

logger = get_logger()


class SynapseExtractor(MssqlExtractor):
    """Synapse metadata extractor"""

    _description = "Synapse metadata crawler"
    _platform = Platform.SYNAPSE
    _dataset_platform = DataPlatform.SYNAPSE

    @staticmethod
    def from_config_file(config_file: str) -> "SynapseExtractor":
        return SynapseExtractor(SynapseConfig.from_yaml_file(config_file))

    def __init__(self, config: SynapseConfig):
        super().__init__(config)
        self._config = config
        self._filter = config.filter.normalize()
        self._lookback_days = config.query_log.lookback_days if config.query_log else 0
        sql_query_endpoint = f"{self._config.server_name}.sql.azuresynapse.net"
        sql_on_demand_query_endpoint = (
            f"{self._config.server_name}-ondemand.sql.azuresynapse.net"
        )
        self._serverless_client = MssqlClient(
            sql_on_demand_query_endpoint, self._config.username, self._config.password
        )
        self._dedicated_client = MssqlClient(
            sql_query_endpoint, self._config.username, self._config.password
        )
        self._dedicated_databases: Set[str] = set()

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(
            f"Fetching metadata from Synapse workspace: {self._config.server_name}"
        )

        entities: List[ENTITY_TYPES] = []

        # Serverless sqlpool
        try:
            for database in self._serverless_client.get_databases():
                if not self._filter.include_database(database.name):
                    continue
                tables = self._serverless_client.get_tables(database.name)
                datasets = self._map_tables_to_dataset(
                    self._config.server_name, database, tables
                )
                self._set_foreign_keys_to_dataset(
                    datasets, database.name, self._serverless_client
                )
                entities.extend(datasets.values())
        except Exception as error:
            logger.exception(f"serverless sqlpool error: {error}")

        # Dedicated sqlpool
        try:
            for database in self._dedicated_client.get_databases():
                if not self._filter.include_database(database.name):
                    continue
                self._dedicated_databases.add(database.name)
                tables = self._dedicated_client.get_tables(database.name)
                datasets = self._map_tables_to_dataset(
                    self._config.server_name, database, tables
                )
                self._set_foreign_keys_to_dataset(
                    datasets, database.name, self._dedicated_client
                )
                entities.extend(datasets.values())
        except Exception as error:
            logger.exception(f"dedicated sqlpool error: {error}")

        return entities

    def collect_query_logs(self) -> Iterator[QueryLog]:
        """
        Prerequisite: `extract` must be called before this method is called.
        """
        if self._lookback_days <= 0:
            return

        start_date = start_of_day(self._lookback_days)
        end_date = start_of_day()
        mapped_log_count = 0

        for log in self._map_query_log(
            WorkspaceQuery.get_sql_pool_query_logs(
                self._serverless_client.config,
                start_date,
                end_date,
            )
        ):
            yield log
            mapped_log_count += 1

        for database in self._dedicated_databases:
            for log in self._map_query_log(
                WorkspaceQuery.get_dedicated_sql_pool_query_logs(
                    self._dedicated_client.config,
                    database,
                    start_date,
                    end_date,
                ),
                database,
            ):
                yield log
                mapped_log_count += 1

        logger.info(f"Wrote {mapped_log_count} QueryLog")

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
        "CREATE": LogType.CREATE_TABLE,
        "SELECT": LogType.SELECT,
        "UPDATE": LogType.UPDATE,
        "DROP": LogType.DROP_TABLE,
        "ALTER": LogType.ALTER_TABLE,
        "TRUNCATE": LogType.TRUNCATE,
        "INSERT": LogType.INSERT,
        "DELETE": LogType.DELETE,
    }

    @staticmethod
    def _map_query_type(operation: str) -> LogType:
        return SynapseExtractor._query_type_map.get(operation.upper(), LogType.OTHER)
