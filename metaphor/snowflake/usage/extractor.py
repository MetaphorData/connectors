import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    MetadataChangeEvent,
)
from serde import deserialize
from serde.json import from_json

from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatabaseFilter, include_table
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.snowflake.auth import connect
from metaphor.snowflake.usage.config import SnowflakeUsageRunConfig
from metaphor.snowflake.utils import QueryWithParam, async_execute

logger = get_logger(__name__)

# disable logging from sql_metadata
logging.getLogger("Parser").setLevel(logging.CRITICAL)


@deserialize
@dataclass
class AccessedObjectColumn:
    columnId: int
    columnName: str


@deserialize
@dataclass
class AccessedObject:
    objectDomain: str
    objectName: str
    objectId: int
    columns: List[AccessedObjectColumn]


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
        self._datasets: Dict[str, Dataset] = {}

    async def extract(
        self, config: SnowflakeUsageRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeUsageExtractor.config_class())

        logger.info("Fetching usage info from Snowflake")

        self.account = config.account
        self.max_concurrency = config.max_concurrency
        self.batch_size = config.batch_size

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

        start_date = datetime.utcnow().date() - timedelta(config.lookback_days)

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
                    SELECT q.QUERY_ID, START_TIME, DIRECT_OBJECTS_ACCESSED
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                    JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                      ON a.QUERY_ID = q.QUERY_ID
                    WHERE EXECUTION_STATUS = 'SUCCESS' and START_TIME > %s
                      {excluded_usernames_clause}
                    ORDER BY q.QUERY_ID
                    LIMIT {self.batch_size} OFFSET %s
                    """,
                    (
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

        # calculate statistics based on the counts
        UsageUtil.calculate_statistics(self._datasets.values())

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _parse_access_logs(self, batch_number: str, access_logs: List[Tuple]) -> None:
        logger.info(f"access logs batch #{batch_number}")
        for query_id, start_time, accessed_objects in access_logs:
            self._parse_access_log(start_time, accessed_objects)

    def _parse_access_log(self, start_time: datetime, accessed_objects: str) -> None:
        try:
            objects = from_json(List[AccessedObject], accessed_objects)
            for obj in objects:
                table_name = obj.objectName.lower()
                parts = table_name.split(".")
                db, schema, table = parts[0], parts[1], parts[2]
                if not include_table(db, schema, table, self.filter):
                    logger.debug(f"Ignore {table_name} due to filter config")
                    continue

                if table_name not in self._datasets:
                    self._datasets[table_name] = UsageUtil.init_dataset(
                        self.account, table_name, DataPlatform.SNOWFLAKE
                    )

                columns = [column.columnName.lower() for column in obj.columns]

                utc_now = datetime.now().replace(tzinfo=timezone.utc)
                UsageUtil.update_table_and_columns_usage(
                    self._datasets[table_name].usage, columns, start_time, utc_now
                )
        except Exception as e:
            logger.error(f"access log error, objects: {accessed_objects}")
            logger.exception(e)
