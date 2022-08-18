from datetime import timedelta
from typing import Collection, List

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.query_history import TableQueryHistoryHeap
from metaphor.common.utils import start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetQueryHistory,
    QueryInfo,
)
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.access_event import AccessEvent
from metaphor.redshift.query.config import RedshiftQueryRunConfig

logger = get_logger(__name__)


class RedshiftQueryExtractor(PostgreSQLExtractor):
    """Redshift query extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftQueryExtractor":
        return RedshiftQueryExtractor(
            RedshiftQueryRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: RedshiftQueryRunConfig):
        super().__init__(
            config,
            "Redshift recent queries crawler",
            Platform.REDSHIFT,
            DataPlatform.REDSHIFT,
        )
        self._lookback_days = config.lookback_days
        self._max_queries_per_table = config.max_queries_per_table
        self._excluded_usernames = config.excluded_usernames

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from redshift host {self._host}")

        self._table_queries = TableQueryHistoryHeap(self._max_queries_per_table)

        utc_now = start_of_day()
        start, end = utc_now - timedelta(self._lookback_days), utc_now

        conn = await self._connect_database(self._database)
        async for record in AccessEvent.fetch_access_event(conn, start, end):
            self._process_record(record)

        return [
            self._init_dataset(table_name, recent_queries)
            for table_name, recent_queries in self._table_queries.recent_queries()
        ]

    def _process_record(self, access_event: AccessEvent):
        if not self._filter.include_table(
            access_event.database, access_event.schema, access_event.table
        ):
            return

        if access_event.usename in self._excluded_usernames:
            return

        table_name = access_event.table_name()

        self._table_queries.store_recent_query(
            table_name,
            QueryInfo(
                elapsed_time=(
                    access_event.endtime - access_event.starttime
                ).total_seconds(),
                issued_at=access_event.starttime,
                issued_by=access_event.usename,
                query=access_event.querytxt,
            ),
        )

    def _init_dataset(
        self, table_name: str, recent_queries: List[QueryInfo]
    ) -> Dataset:
        return Dataset(
            logical_id=DatasetLogicalID(
                name=table_name, platform=DataPlatform.REDSHIFT
            ),
            query_history=DatasetQueryHistory(recent_queries=recent_queries),
        )
