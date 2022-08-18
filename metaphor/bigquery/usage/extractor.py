from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Collection, Dict, List, Optional

from metaphor.bigquery.usage.config import BigQueryUsageRunConfig
from metaphor.bigquery.utils import BigQueryResource, LogEntry, build_logging_client
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.common.utils import start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform, Dataset

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

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryUsageExtractor":
        return BigQueryUsageExtractor(
            BigQueryUsageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: BigQueryUsageRunConfig):
        super().__init__(config, "BigQuery usage statistics crawler", Platform.BIGQUERY)
        self._client = build_logging_client(config)
        self._dataset_filter = config.filter.normalize()
        self._excluded_usernames = config.excluded_usernames
        self._use_history = config.use_history
        self._lookback_days = config.lookback_days
        self._batch_size = config.batch_size

        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:

        logger.info("Fetching usage info from BigQuery")

        utc_now = start_of_day()

        log_filter = self._build_table_data_read_filter(utc_now)
        counter = 0
        for entry in self._client.list_entries(
            page_size=self._batch_size, filter_=log_filter
        ):
            counter += 1
            if TableReadEvent.can_parse(entry):
                self._parse_table_data_read_entry(entry, utc_now)

            if counter % 1000 == 0:
                logger.info(f"Fetched {counter} audit logs")

        logger.info(f"Number of tableDataRead log entries fetched: {counter}")

        if not self._use_history:
            UsageUtil.calculate_statistics(self._datasets.values())

        return self._datasets.values()

    def _parse_table_data_read_entry(self, entry: LogEntry, utc_now: datetime) -> None:
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
                None, table_name, DataPlatform.BIGQUERY, self._use_history, utc_now
            )

        if self._use_history:
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
                utc_now=utc_now,
                username=read_event.username,
            )

    def _build_table_data_read_filter(self, end_time: datetime) -> str:
        lookback_days = 1 if self._use_history else self._lookback_days

        start = (end_time - timedelta(days=lookback_days)).isoformat()
        end = end_time.isoformat()

        return f"""
        resource.type="bigquery_dataset" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.tableDataRead.reason = "JOB" AND
        timestamp >= "{start}" AND
        timestamp < "{end}"
        """
