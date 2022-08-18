from datetime import datetime, timedelta
from typing import Collection

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.common.utils import start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.access_event import AccessEvent
from metaphor.redshift.usage.config import RedshiftUsageRunConfig

logger = get_logger(__name__)


class RedshiftUsageExtractor(PostgreSQLExtractor):
    """Redshift usage metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftUsageExtractor":
        return RedshiftUsageExtractor(
            RedshiftUsageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: RedshiftUsageRunConfig):
        super().__init__(
            config,
            "Redshift usage statistics crawler",
            Platform.REDSHIFT,
            DataPlatform.REDSHIFT,
        )
        self._use_history = config.use_history
        self._lookback_days = config.lookback_days
        self._excluded_usernames = config.excluded_usernames

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from redshift host {self._host}")

        utc_now = start_of_day()
        lookback_days = 1 if self._use_history else self._lookback_days
        start, end = utc_now - timedelta(lookback_days), utc_now

        conn = await self._connect_database(self._database)
        async for record in AccessEvent.fetch_access_event(conn, start, end):
            self._process_record(record, utc_now)

        if not self._use_history:
            UsageUtil.calculate_statistics(self._datasets.values())

        return self._datasets.values()

    def _process_record(self, access_event: AccessEvent, utc_now: datetime):

        if not self._filter.include_table(
            access_event.database, access_event.schema, access_event.table
        ):
            return

        if access_event.usename in self._excluded_usernames:
            return

        table_name = access_event.table_name()
        if table_name not in self._datasets:
            self._datasets[table_name] = UsageUtil.init_dataset(
                None, table_name, DataPlatform.REDSHIFT, self._use_history, utc_now
            )

        if self._use_history:
            UsageUtil.update_table_and_columns_usage_history(
                self._datasets[table_name].usage_history, [], access_event.usename
            )
        else:
            UsageUtil.update_table_and_columns_usage(
                usage=self._datasets[table_name].usage,
                columns=[],
                start_time=access_event.starttime,
                utc_now=utc_now,
                username=access_event.usename,
            )
