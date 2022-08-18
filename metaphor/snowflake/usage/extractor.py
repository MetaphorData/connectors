import logging
import math
from datetime import datetime
from typing import Collection, Dict, List, Tuple

from pydantic import parse_raw_as

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatabaseFilter
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.common.utils import start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform, Dataset
from metaphor.snowflake import auth
from metaphor.snowflake.accessed_object import AccessedObject
from metaphor.snowflake.usage.config import SnowflakeUsageRunConfig
from metaphor.snowflake.utils import (
    QueryWithParam,
    async_execute,
    fetch_query_history_count,
)

logger = get_logger(__name__)

# disable logging from sql_metadata
logging.getLogger("Parser").setLevel(logging.CRITICAL)


DEFAULT_EXCLUDED_DATABASES: DatabaseFilter = {"SNOWFLAKE": None}


class SnowflakeUsageExtractor(BaseExtractor):
    """Snowflake usage metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SnowflakeUsageExtractor":
        return SnowflakeUsageExtractor(
            SnowflakeUsageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: SnowflakeUsageRunConfig):
        super().__init__(
            config, "Snowflake usage statistics crawler", Platform.SNOWFLAKE
        )
        self._account = config.account
        self._filter = config.filter.normalize()
        self._max_concurrency = config.max_concurrency
        self._batch_size = config.batch_size
        self._use_history = config.use_history
        self._excluded_usernames = config.excluded_usernames
        self._lookback_days = 1 if config.use_history else config.lookback_days

        self._conn = auth.connect(config)
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:

        logger.info("Fetching usage info from Snowflake")

        self._filter.excludes = (
            DEFAULT_EXCLUDED_DATABASES
            if self._filter.excludes is None
            else {**self._filter.excludes, **DEFAULT_EXCLUDED_DATABASES}
        )

        excluded_usernames_clause = (
            f"and q.USER_NAME NOT IN ({','.join(['%s'] * len(self._excluded_usernames))})"
            if len(self._excluded_usernames) > 0
            else ""
        )

        start_date = start_of_day(self._lookback_days)

        with self._conn:
            count = fetch_query_history_count(
                self._conn, start_date, self._excluded_usernames
            )
            batches = math.ceil(count / self._batch_size)
            logger.info(f"Total {count} queries, dividing into {batches} batches")

            queries = {
                x: QueryWithParam(
                    f"""
                    SELECT START_TIME, q.USER_NAME, DIRECT_OBJECTS_ACCESSED
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                    JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                      ON a.QUERY_ID = q.QUERY_ID
                    WHERE EXECUTION_STATUS = 'SUCCESS'
                      AND START_TIME > %s
                      AND QUERY_START_TIME > %s
                      {excluded_usernames_clause}
                    ORDER BY q.QUERY_ID
                    LIMIT {self._batch_size} OFFSET %s
                    """,
                    (
                        start_date,
                        start_date,
                        *self._excluded_usernames,
                        x * self._batch_size,
                    ),
                )
                for x in range(batches)
            }
            async_execute(
                self._conn,
                queries,
                "fetch_access_logs",
                self._max_concurrency,
                self._parse_access_logs,
            )

        if not self._use_history:
            # calculate statistics based on the counts
            UsageUtil.calculate_statistics(self._datasets.values())

        return self._datasets.values()

    def _parse_access_logs(self, batch_number: str, access_logs: List[Tuple]) -> None:
        logger.info(f"access logs batch #{batch_number}")
        for start_time, username, accessed_objects in access_logs:
            self._parse_access_log(start_time, username, accessed_objects)

    def _parse_access_log(
        self, start_time: datetime, username: str, accessed_objects: str
    ) -> None:
        try:
            objects = parse_raw_as(List[AccessedObject], accessed_objects)
            for obj in objects:
                if not obj.objectDomain or obj.objectDomain.upper() not in (
                    "TABLE",
                    "VIEW",
                    "MATERIALIZED VIEW",
                ):
                    continue

                table_name = obj.objectName.lower()
                parts = table_name.split(".")
                if len(parts) != 3:
                    logger.debug(f"Invalid table name {table_name}, skip")
                    continue

                db, schema, table = parts[0], parts[1], parts[2]
                if not self._filter.include_table(db, schema, table):
                    logger.debug(f"Ignore {table_name} due to filter config")
                    continue

                utc_now = start_of_day()

                if table_name not in self._datasets:
                    self._datasets[table_name] = UsageUtil.init_dataset(
                        self._account,
                        table_name,
                        DataPlatform.SNOWFLAKE,
                        self._use_history,
                        utc_now,
                    )

                columns = [column.columnName.lower() for column in obj.columns]

                if self._use_history:
                    UsageUtil.update_table_and_columns_usage_history(
                        self._datasets[table_name].usage_history, columns, username
                    )
                else:
                    UsageUtil.update_table_and_columns_usage(
                        self._datasets[table_name].usage,
                        columns,
                        start_time,
                        utc_now,
                        username,
                    )
        except Exception:
            logger.exception(f"access log error, objects: {accessed_objects}")
