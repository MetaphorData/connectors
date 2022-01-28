import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    MetadataChangeEvent,
)

from metaphor.bigquery.usage.config import BigQueryUsageRunConfig
from metaphor.bigquery.utils import BigQueryResource, LogEntry, build_logging_client
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil

logger = get_logger(__name__)
logger.setLevel(logging.INFO)


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
    def from_entry(entry: LogEntry) -> "TableReadEvent":
        timestamp = entry.received_timestamp
        table_read = entry.payload["metadata"]["tableDataRead"]
        bq_resource = BigQueryResource.from_str(entry.payload["resourceName"])
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
    def config_class():
        return BigQueryUsageRunConfig

    def __init__(self):
        self._utc_now = datetime.now().replace(tzinfo=timezone.utc)
        self._datasets: Dict[str, Dataset] = {}
        self._datasets_pattern: List[re.Pattern[str]] = []
        self._dataset_filter: DatasetFilter = DatasetFilter()
        self._excluded_usernames: Set[str] = set()

    async def extract(
        self, config: BigQueryUsageRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, BigQueryUsageExtractor.config_class())

        logger.info("Fetching usage info from BigQuery")

        client = build_logging_client(config.key_path, config.project_id)
        self._dataset_filter = config.filter.normalize()

        log_filter = self._build_table_data_read_filter(config, end_time=self._utc_now)
        counter = 0
        for entry in client.list_entries(
            page_size=config.batch_size, filter_=log_filter
        ):
            counter += 1
            if TableReadEvent.can_parse(entry):
                self._parse_table_data_read_entry(entry)

        logger.info(f"Number of tableDataRead log entries fetched: {counter}")
        UsageUtil.calculate_statistics(self._datasets.values())

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _parse_table_data_read_entry(self, entry: LogEntry):
        read_event = TableReadEvent.from_entry(entry)

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
                None, table_name, DataPlatform.BIGQUERY
            )

        usage = self._datasets[table_name].usage
        UsageUtil.update_table_and_columns_usage(
            usage=usage,
            columns=read_event.columns,
            start_time=read_event.timestamp,
            utc_now=self._utc_now,
        )

    @staticmethod
    def _build_table_data_read_filter(config: BigQueryUsageRunConfig, end_time):
        start = (end_time - timedelta(days=config.lookback_days)).isoformat()
        end = end_time.isoformat()

        return f"""
        resource.type="bigquery_dataset" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.tableDataRead.reason = "JOB" AND
        timestamp >= "{start}" AND
        timestamp < "{end}"
        """
