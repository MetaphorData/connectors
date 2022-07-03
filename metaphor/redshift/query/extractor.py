from datetime import timedelta
from typing import Collection, List, Optional, Set

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetQueryHistory,
    QueryInfo,
)

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.query_history import TableQueryHistoryHeap
from metaphor.common.utils import start_of_day
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.access_event import AccessEvent
from metaphor.redshift.query.config import RedshiftQueryRunConfig

logger = get_logger(__name__)


class RedshiftQueryExtractor(PostgreSQLExtractor):
    """Redshift query extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.REDSHIFT

    def description(self) -> str:
        return "Redshift recent queries crawler"

    @staticmethod
    def config_class():
        return RedshiftQueryRunConfig

    def __init__(self):
        super().__init__()
        self._utc_now = start_of_day()
        self._excluded_usernames: Set[str] = set()

    async def extract(self, config: RedshiftQueryRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, PostgreSQLExtractor.config_class())

        logger.info(f"Fetching metadata from redshift host {config.host}")

        dataset_filter = DatasetFilter.normalize(config.filter)
        self._table_queries = TableQueryHistoryHeap(config.max_queries_per_table)

        start, end = self._utc_now - timedelta(config.lookback_days), self._utc_now

        async for record in AccessEvent.fetch_access_event(
            config, config.database, start, end
        ):
            self._process_record(record, dataset_filter)

        return [
            self._init_dataset(table_name, recent_queries)
            for table_name, recent_queries in self._table_queries.recent_queries()
        ]

    def _process_record(self, access_event: AccessEvent, dataset_filter: DatasetFilter):
        if not dataset_filter.include_table(
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
