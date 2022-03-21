import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Collection, Dict, Set

from asyncpg import Record
from metaphor.models.metadata_change_event import DataPlatform, Dataset

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.common.utils import start_of_day
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.usage.config import RedshiftUsageRunConfig

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

REDSHIFT_USAGE_SQL_TEMPLATE = """
SELECT DISTINCT ss.userid,
    ss.query,
    sui.usename,
    ss.tbl,
    sq.querytxt,
    sti.database,
    sti.schema,
    sti.table,
    sq.starttime,
    sq.endtime,
    sq.aborted
FROM stl_scan ss
    JOIN svv_table_info sti ON ss.tbl = sti.table_id
    JOIN stl_query sq ON ss.query = sq.query
    JOIN svl_user_info sui ON sq.userid = sui.usesysid
WHERE ss.starttime >= '{start_time}'
    AND ss.starttime < '{end_time}'
    AND sq.aborted = 0
ORDER BY ss.endtime DESC;
"""


@dataclass
class AccessEvent:
    userid: int
    query: int
    usename: str
    tbl: int
    querytxt: str
    database: str
    schema: str
    table: str
    starttime: datetime
    endtime: datetime
    aborted: int

    @staticmethod
    def from_record(record: Record) -> "AccessEvent":
        # To convert mock Record to a dict
        if hasattr(record, "_asdict"):
            record = dict(record._asdict())
        else:
            record = dict(record)

        for k, v in record.items():
            if isinstance(v, str):
                record[k] = v.strip()

        record["starttime"] = record["starttime"].replace(tzinfo=timezone.utc)
        record["endtime"] = record["endtime"].replace(tzinfo=timezone.utc)

        return AccessEvent(**record)

    def table_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


class RedshiftUsageExtractor(PostgreSQLExtractor):
    """Redshift usage metadata extractor"""

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

        filter = DatasetFilter.normalize(config.filter)

        async for record in self._fetch_usage(config):
            self._process_record(record, filter, config.use_history)

        if not config.use_history:
            UsageUtil.calculate_statistics(self._datasets.values())

        return self._datasets.values()

    async def _fetch_usage(
        self, config: RedshiftUsageRunConfig
    ) -> AsyncIterator[Record]:
        conn = await PostgreSQLExtractor._connect_database(config, config.database)

        lookback_days = 1 if config.use_history else config.lookback_days
        start = (self._utc_now - timedelta(lookback_days)).isoformat()
        end = self._utc_now.isoformat()

        results = await conn.fetch(
            REDSHIFT_USAGE_SQL_TEMPLATE.format(start_time=start, end_time=end)
        )

        for record in results:
            yield record

        await conn.close()

    def _process_record(self, record: Record, filter: DatasetFilter, use_history: bool):
        access_event = AccessEvent.from_record(record)

        if not filter.include_table(
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
                self._datasets[table_name].usage_history, []
            )
        else:
            UsageUtil.update_table_and_columns_usage(
                usage=self._datasets[table_name].usage,
                columns=[],
                start_time=access_event.starttime,
                utc_now=self._utc_now,
            )
