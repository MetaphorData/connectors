from datetime import timedelta
from typing import Collection, Dict, Optional, Set

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform, Dataset

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.common.utils import start_of_day
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.access_event import AccessEvent
from metaphor.redshift.usage.config import RedshiftUsageRunConfig

logger = get_logger(__name__)


class RedshiftUsageExtractor(PostgreSQLExtractor):
    """Redshift usage metadata extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.REDSHIFT

    def description(self) -> str:
        return "Redshift usage statistics crawler"

    @staticmethod
    def config_class():
        return RedshiftUsageRunConfig

    def __init__(self):
        super().__init__()
        self._utc_now = start_of_day()
        self._datasets: Dict[str, Dataset] = {}
        self._excluded_usernames: Set[str] = set()

    async def extract(self, config: RedshiftUsageRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, PostgreSQLExtractor.config_class())

        logger.info(f"Fetching metadata from redshift host {config.host}")

        dataset_filter = DatasetFilter.normalize(config.filter)

        lookback_days = 1 if config.use_history else config.lookback_days
        start, end = self._utc_now - timedelta(lookback_days), self._utc_now

        async for record in AccessEvent.fetch_access_event(
            config, config.database, start, end
        ):
            self._process_record(record, dataset_filter, config.use_history)

        if not config.use_history:
            UsageUtil.calculate_statistics(self._datasets.values())

        return self._datasets.values()

    def _process_record(
        self,
        access_event: AccessEvent,
        dataset_filter: DatasetFilter,
        use_history: bool,
    ):

        if not dataset_filter.include_table(
            access_event.database, access_event.schema, access_event.table
        ):
            return

        if access_event.usename in self._excluded_usernames:
            return

        table_name = access_event.table_name()
        if table_name not in self._datasets:
            self._datasets[table_name] = UsageUtil.init_dataset(
                None, table_name, DataPlatform.REDSHIFT, use_history, self._utc_now
            )

        if use_history:
            UsageUtil.update_table_and_columns_usage_history(
                self._datasets[table_name].usage_history, [], access_event.usename
            )
        else:
            UsageUtil.update_table_and_columns_usage(
                usage=self._datasets[table_name].usage,
                columns=[],
                start_time=access_event.starttime,
                utc_now=self._utc_now,
                username=access_event.usename,
            )
