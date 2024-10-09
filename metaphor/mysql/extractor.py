from csv import reader
from typing import Collection, Iterator, List, Optional

from metaphor.common.aws import iterate_logs_from_cloud_watch
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.sql.query_log import PartialQueryLog, process_and_init_query_log
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.sql.utils import is_valid_queried_datasets
from metaphor.common.utils import to_utc_datetime_from_timestamp
from metaphor.database.extractor import GenericDatabaseExtractor
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform, QueryLog
from metaphor.mysql.config import MySQLQueryLogConfig, MySQLRunConfig

logger = get_logger()


class MySQLExtractor(GenericDatabaseExtractor):
    """MySQL metadata extractor"""

    _description = "MySQL metadata crawler"
    _platform = Platform.MYSQL
    _sqlalchemy_dialect = "mysql+pymysql"

    @staticmethod
    def from_config_file(config_file: str) -> "MySQLExtractor":
        return MySQLExtractor(MySQLRunConfig.from_yaml_file(config_file))

    def __init__(self, config: MySQLRunConfig):
        super().__init__(config)

        self._query_log_config = config.query_log

        self._query_log_config.excluded_usernames.add("rdsadmin")

        # MySql should only use schema.table
        self._database = None

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from MySQL host {self._config.host}")

        inspector = MySQLExtractor.get_inspector(self._get_sqlalchemy_url())
        for schema in self._get_schemas(inspector):
            self._extract_schema(inspector, schema)
        self._extract_foreign_key(inspector)

        return self._datasets.values()

    def collect_query_logs(self) -> Iterator[QueryLog]:
        if (
            isinstance(self._query_log_config, MySQLQueryLogConfig)
            and self._query_log_config.aws
            and self._query_log_config.lookback_days > 0
            and self._query_log_config.logs_group is not None
        ):
            client = self._query_log_config.aws.get_session().client("logs")
            lookback_days = self._query_log_config.lookback_days
            logs_group = self._query_log_config.logs_group

            logs = iterate_logs_from_cloud_watch(client, lookback_days, logs_group)
            for row in reader(logs, delimiter=",", quotechar="'", escapechar="\\"):
                query_log = self._process_cloud_watch_log(row)
                if query_log:
                    yield query_log

    def _process_cloud_watch_log(self, row: List[str]) -> Optional[QueryLog]:

        # example row:
        # 1728313380329971,server,rdsadmin,localhost,207,15268,QUERY,db,'SELECT 1',0
        (timestamp_us, user, statement_type, database, query) = (
            row[0],
            row[2],
            row[6],
            row[7],
            row[8],
        )

        if not user or user in self._query_log_config.excluded_usernames:
            return None

        if statement_type != "QUERY":
            logger.debug(f"Skip processing statement type: {statement_type}")

        tll = extract_table_level_lineage(
            sql=query,
            platform=DataPlatform.MYSQL,
            account=self._alternative_host or self._config.host,
            default_schema=database if database else None,
        )

        # Skip if parsed sources or targets has invalid data.
        if not is_valid_queried_datasets(
            tll.sources, ignore_database=True
        ) or not is_valid_queried_datasets(tll.targets, ignore_database=True):
            return None

        return process_and_init_query_log(
            query=query,
            platform=DataPlatform.MYSQL,
            process_query_config=self._query_log_config.process_query,
            query_log=PartialQueryLog(
                default_schema=database if database else None,
                user_id=user,
                start_time=to_utc_datetime_from_timestamp(int(timestamp_us) / 1000_000),
                sources=tll.sources,
                targets=tll.targets,
            ),
        )
