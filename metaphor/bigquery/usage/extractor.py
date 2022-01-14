import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Set

from smart_open import open

try:
    from google.cloud import logging_v2
    from google.oauth2 import service_account
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    MetadataChangeEvent,
)

from metaphor.bigquery.usage.config import BigQueryUsageRunConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil

logger = get_logger(__name__)
logger.setLevel(logging.INFO)


# ProtobufEntry is a namedtuple and attribute assigned dynamically with different type, mypy fail here
# See: https://googleapis.dev/python/logging/latest/client.html#google.cloud.logging_v2.client.Client.list_entries
#
# from google.cloud.logging_v2 import ProtobufEntry
# LogEntry = ProtobufEntry
LogEntry = Any


def build_client(config: BigQueryUsageRunConfig):
    with open(config.key_path) as fin:
        credentials = service_account.Credentials.from_service_account_info(
            json.load(fin),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        return logging_v2.Client(
            credentials=credentials,
            project=config.project_id if config.project_id else credentials.project_id,
        )


@dataclass
class BigQueryResource:
    project_id: str
    dataset_id: str
    table_id: str

    @staticmethod
    def from_str(resource_name: str) -> "BigQueryResource":
        _, project_id, _, dataset_id, _, table_id = resource_name.split("/")
        return BigQueryResource(project_id, dataset_id, table_id)

    def table_name(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"


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
        self._excluded_usernames: Set[str] = set()

    async def extract(
        self, config: BigQueryUsageRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, BigQueryUsageExtractor.config_class())

        logger.info("Fetching usage info from BigQuery")

        client = build_client(config)
        filter = self.build_filter(config, end_time=self._utc_now)

        self._datasets_pattern = [re.compile(f) for f in config.dataset_filters]

        counter = 0
        for entry in client.list_entries(page_size=config.batch_size, filter_=filter):
            counter += 1
            if TableReadEvent.can_parse(entry):
                self._parse_log_entry(entry)

        logger.info(f"Number of log entries fetched: {counter}")
        UsageUtil.calculate_statistics(self._datasets.values())

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _parse_log_entry(self, entry: LogEntry):
        read_event = TableReadEvent.from_entry(entry)

        def match_patterns(dataset_id, patterns):
            for pattern in patterns:
                if re.fullmatch(pattern, dataset_id) is not None:
                    return True
            return False

        if not match_patterns(read_event.resource.dataset_id, self._datasets_pattern):
            return

        if read_event.username in self._excluded_usernames:
            return

        table_name = read_event.resource.table_name()
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
    def build_filter(config: BigQueryUsageRunConfig, end_time):
        start = (end_time - timedelta(days=config.lookback_days)).isoformat()
        end = end_time.isoformat()

        return f"""
        resource.type="bigquery_dataset" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.tableDataRead.reason = "JOB" AND
        timestamp >= "{start}" AND
        timestamp < "{end}"
        """
