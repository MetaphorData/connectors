import logging
import math
from datetime import datetime
from typing import Collection, Dict, List, Tuple

from metaphor.models.metadata_change_event import DataPlatform, Dataset
from pydantic import parse_raw_as

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatabaseFilter
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.common.utils import start_of_day
from metaphor.snowflake.accessed_object import AccessedObject
from metaphor.snowflake.auth import connect
from metaphor.snowflake.usage.config import SnowflakeUsageRunConfig
from metaphor.snowflake.utils import QueryWithParam, async_execute

logger = get_logger(__name__)

# disable logging from sql_metadata
logging.getLogger("Parser").setLevel(logging.CRITICAL)


DEFAULT_EXCLUDED_DATABASES: DatabaseFilter = {"SNOWFLAKE": None}


class SnowflakeUsageExtractor(BaseExtractor):
    """Snowflake usage metadata extractor"""

    @staticmethod
    def config_class():
        return SnowflakeUsageRunConfig

    def __init__(self):
        self.account = None
        self.filter = None
        self.max_concurrency = None
        self.batch_size = None
        self._use_history = True
        self._datasets: Dict[str, Dataset] = {}

    async def extract(
        self, config: SnowflakeUsageRunConfig
    ) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, SnowflakeUsageExtractor.config_class())

        logger.info("Fetching usage info from Snowflake")

        self.account = config.account
        self.max_concurrency = config.max_concurrency
        self.batch_size = config.batch_size
        self._use_history = config.use_history

        self.filter = config.filter.normalize()

        self.filter.excludes = (
            DEFAULT_EXCLUDED_DATABASES
            if self.filter.excludes is None
            else {**self.filter.excludes, **DEFAULT_EXCLUDED_DATABASES}
        )

        excluded_usernames_clause = (
            f"and USER_NAME NOT IN ({','.join(['%s'] * len(config.excluded_usernames))})"
            if len(config.excluded_usernames) > 0
            else ""
        )

        lookback_days = 1 if config.use_history else config.lookback_days
        start_date = start_of_day(lookback_days)

        conn = connect(config)

        with conn:
            cursor = conn.cursor()

            cursor.execute(
                f"""
                SELECT COUNT(1)
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE EXECUTION_STATUS = 'SUCCESS' and START_TIME > %s
                  {excluded_usernames_clause}
                """,
                (
                    start_date,
                    *config.excluded_usernames,
                ),
            )
            count = cursor.fetchone()[0]
            batches = math.ceil(count / self.batch_size)
            logger.info(f"Total {count} queries, dividing into {batches} batches")

            queries = {
                x: QueryWithParam(
                    f"""
                    SELECT START_TIME, DIRECT_OBJECTS_ACCESSED
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                    JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                      ON a.QUERY_ID = q.QUERY_ID
                    WHERE EXECUTION_STATUS = 'SUCCESS'
                      AND START_TIME > %s
                      AND QUERY_START_TIME > %s
                      {excluded_usernames_clause}
                    ORDER BY q.QUERY_ID
                    LIMIT {self.batch_size} OFFSET %s
                    """,
                    (
                        start_date,
                        start_date,
                        *config.excluded_usernames,
                        x * self.batch_size,
                    ),
                )
                for x in range(batches)
            }
            async_execute(
                conn,
                queries,
                "fetch_access_logs",
                self.max_concurrency,
                self._parse_access_logs,
            )

        if not self._use_history:
            # calculate statistics based on the counts
            UsageUtil.calculate_statistics(self._datasets.values())

        return self._datasets.values()

    def _parse_access_logs(self, batch_number: str, access_logs: List[Tuple]) -> None:
        logger.info(f"access logs batch #{batch_number}")
        for start_time, accessed_objects in access_logs:
            self._parse_access_log(start_time, accessed_objects)

    def _parse_access_log(self, start_time: datetime, accessed_objects: str) -> None:
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
                if not self.filter.include_table(db, schema, table):
                    logger.debug(f"Ignore {table_name} due to filter config")
                    continue

                utc_now = start_of_day()

                if table_name not in self._datasets:
                    self._datasets[table_name] = UsageUtil.init_dataset(
                        self.account,
                        table_name,
                        DataPlatform.SNOWFLAKE,
                        self._use_history,
                        utc_now,
                    )

                columns = [column.columnName.lower() for column in obj.columns]

                if self._use_history:
                    UsageUtil.update_table_and_columns_usage_history(
                        self._datasets[table_name].usage_history, columns
                    )
                else:
                    UsageUtil.update_table_and_columns_usage(
                        self._datasets[table_name].usage, columns, start_time, utc_now
                    )
        except Exception:
            logger.exception(f"access log error, objects: {accessed_objects}")
