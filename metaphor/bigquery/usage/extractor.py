import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Collection, Dict, List, Optional, Set

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform, Dataset

from metaphor.bigquery.usage.config import BigQueryUsageRunConfig
from metaphor.bigquery.utils import BigQueryResource, LogEntry, build_logging_client
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.common.utils import start_of_day

logger = get_logger(__name__)


@dataclass
class TableReadEvent:
    timestamp: datetime
    resource: BigQueryResource
    columns: List[str]
    username: str

    @staticmethod
    def can_parse(entry: LogEntry) -> bool:
        try:
            if entry.resource.type != "bigquery_dataset":
                return False
            assert isinstance(entry.received_timestamp, datetime)
            assert isinstance(entry.payload["metadata"]["tableDataRead"], dict)
            return True
        except KeyError:
            return False

    @staticmethod
    def from_entry(entry: LogEntry) -> Optional["TableReadEvent"]:
        bq_resource = BigQueryResource.from_str(
            entry.payload["resourceName"]
        ).remove_extras()
        if bq_resource.is_temporary():
            return None

        timestamp = entry.received_timestamp
        table_read = entry.payload["metadata"]["tableDataRead"]
        columns = table_read.get("fields", [])
        username = entry.payload["authenticationInfo"].get("principalEmail", "")

        return TableReadEvent(
            timestamp=timestamp,
            resource=bq_resource,
            columns=columns,
            username=username,
        )


class BigQueryUsageExtractor(BaseExtractor):
    """BigQuery usage metadata extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.BIGQUERY

    def description(self) -> str:
        return "BigQuery usage statistics crawler"

    @staticmethod
    def config_class():
        return BigQueryUsageRunConfig

    def __init__(self):
        self._utc_now = start_of_day()
        self._datasets: Dict[str, Dataset] = {}
        self._datasets_pattern: List[re.Pattern[str]] = []
        self._dataset_filter: DatasetFilter = DatasetFilter()
        self._excluded_usernames: Set[str] = set()

    async def extract(self, config: BigQueryUsageRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, BigQueryUsageExtractor.config_class())

        logger.info("Fetching usage info from BigQuery")

        client = build_logging_client(config)
        self._dataset_filter = config.filter.normalize()

        log_filter = self._build_table_data_read_filter(config, end_time=self._utc_now)
        counter = 0
        for entry in client.list_entries(
            page_size=config.batch_size, filter_=log_filter
        ):
            counter += 1
            if TableReadEvent.can_parse(entry):
                self._parse_table_data_read_entry(entry, config.use_history)

            if counter % 1000 == 0:
                logger.info(f"Fetched {counter} audit logs")

        logger.info(f"Number of tableDataRead log entries fetched: {counter}")

        if not config.use_history:
            UsageUtil.calculate_statistics(self._datasets.values())

        return self._datasets.values()

    def _parse_table_data_read_entry(self, entry: LogEntry, use_history: bool) -> None:
        read_event = TableReadEvent.from_entry(entry)
        if read_event is None:
            return

        resource = read_event.resource
        if not self._dataset_filter.include_schema(
            resource.project_id, resource.dataset_id
        ) or not self._dataset_filter.include_table(
            resource.project_id, resource.dataset_id, resource.table_id
        ):
            logger.info(f"Skipped table: {resource.table_name()}")
            return

        if read_event.username in self._excluded_usernames:
            return

        table_name = resource.table_name()
        if table_name not in self._datasets:
            self._datasets[table_name] = UsageUtil.init_dataset(
                None, table_name, DataPlatform.BIGQUERY, use_history, self._utc_now
            )

        if use_history:
            history = self._datasets[table_name].usage_history
            UsageUtil.update_table_and_columns_usage_history(
                history=history,
                columns=read_event.columns,
                username=read_event.username,
            )
        else:
            usage = self._datasets[table_name].usage
            UsageUtil.update_table_and_columns_usage(
                usage=usage,
                columns=read_event.columns,
                start_time=read_event.timestamp,
                utc_now=self._utc_now,
                username=read_event.username,
            )

    @staticmethod
    def _build_table_data_read_filter(config: BigQueryUsageRunConfig, end_time) -> str:
        lookback_days = 1 if config.use_history else config.lookback_days

        start = (end_time - timedelta(days=lookback_days)).isoformat()
        end = end_time.isoformat()

        return f"""
        resource.type="bigquery_dataset" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.tableDataRead.reason = "JOB" AND
        timestamp >= "{start}" AND
        timestamp < "{end}"
        """
