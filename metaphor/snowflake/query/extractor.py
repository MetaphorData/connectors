import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Collection, List, Optional, Tuple

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetQueryHistory,
    QueryInfo,
)
from pydantic import parse_raw_as

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatabaseFilter
from metaphor.common.logger import get_logger
from metaphor.common.query_history import TableQueryHistoryHeap
from metaphor.common.utils import start_of_day
from metaphor.snowflake.accessed_object import AccessedObject
from metaphor.snowflake.auth import connect
from metaphor.snowflake.query.config import SnowflakeQueryRunConfig
from metaphor.snowflake.utils import (
    QueryWithParam,
    async_execute,
    fetch_query_history_count,
)

logger = get_logger(__name__)

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

DEFAULT_EXCLUDED_DATABASES: DatabaseFilter = {"SNOWFLAKE": None}


@dataclass(order=True)
class PrioritizedQueryInfo:
    time: datetime
    item: QueryInfo = field(compare=False)


class SnowflakeQueryExtractor(BaseExtractor):
    """Snowflake query extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.SNOWFLAKE

    def description(self) -> str:
        return "Snowflake recent queries crawler"

    @staticmethod
    def config_class():
        return SnowflakeQueryRunConfig

    def __init__(self):
        self.max_concurrency = None

    async def extract(
        self, config: SnowflakeQueryRunConfig
    ) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, SnowflakeQueryExtractor.config_class())

        logger.info("Fetching query history from Snowflake")
        self.max_concurrency = config.max_concurrency
        self.batch_size = config.batch_size
        self._table_queries = TableQueryHistoryHeap(config.max_queries_per_table)

        conn = connect(config)

        self.filter = config.filter.normalize()

        self.filter.excludes = (
            DEFAULT_EXCLUDED_DATABASES
            if self.filter.excludes is None
            else {**self.filter.excludes, **DEFAULT_EXCLUDED_DATABASES}
        )

        start_date = start_of_day(config.lookback_days)

        excluded_usernames_clause = (
            f"and q.USER_NAME NOT IN ({','.join(['%s'] * len(config.excluded_usernames))})"
            if len(config.excluded_usernames) > 0
            else ""
        )

        with conn:
            count = fetch_query_history_count(
                conn, start_date, config.excluded_usernames
            )
            batches = math.ceil(count / self.batch_size)
            logger.info(f"Total {count} queries, dividing into {batches} batches")

            queries = {
                x: QueryWithParam(
                    f"""
                    SELECT START_TIME, QUERY_TEXT, q.USER_NAME, DIRECT_OBJECTS_ACCESSED, TOTAL_ELAPSED_TIME
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                    JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                      ON a.QUERY_ID = q.QUERY_ID
                    WHERE
                      EXECUTION_STATUS = 'SUCCESS'
                      AND START_TIME > %s
                      AND QUERY_START_TIME > %s
                      {excluded_usernames_clause}
                    ORDER BY q.QUERY_ID DESC
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
                "fetch_query_history",
                self.max_concurrency,
                self._parse_query_history,
            )

        return [
            self._init_dataset(table_name, recent_queries)
            for table_name, recent_queries in self._table_queries.recent_queries()
        ]

    def _parse_query_history(
        self, batch_number: str, query_history: List[Tuple]
    ) -> None:
        logger.info(f"access logs batch #{batch_number}")
        for (
            start_time,
            query_text,
            username,
            accessed_objects,
            total_elapsed_time,
        ) in query_history:
            self._parse_access_log(
                start_time, username, query_text, accessed_objects, total_elapsed_time
            )

    def _parse_access_log(
        self,
        start_time: datetime,
        username: str,
        query_text: str,
        accessed_objects: str,
        total_elapsed_time: float,
    ):
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
                self._table_queries.store_recent_query(
                    table_name,
                    QueryInfo(
                        elapsed_time=total_elapsed_time / 1000,
                        issued_at=start_time,
                        issued_by=username,
                        query=query_text,
                    ),
                )
        except Exception:
            logger.exception(f"access log error, objects: {accessed_objects}")

    def _init_dataset(
        self, table_name: str, recent_queries: List[QueryInfo]
    ) -> Dataset:
        return Dataset(
            logical_id=DatasetLogicalID(
                name=table_name, platform=DataPlatform.SNOWFLAKE
            ),
            query_history=DatasetQueryHistory(recent_queries=recent_queries),
        )
