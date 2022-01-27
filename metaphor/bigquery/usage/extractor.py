import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

from smart_open import open

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.filter import DatasetFilter

try:
    from google.cloud import logging_v2
    from google.oauth2 import service_account
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUpstream,
    EntityType,
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


@dataclass
class JobChangeEvent:
    """
    Container class for BigQueryAuditMetadata.JobChange, where the 'after' job status is 'DONE'
    See https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.jobchange
    """

    job_name: str
    timestamp: datetime
    user_email: str

    query: Optional[str]
    statementType: Optional[str]
    source_tables: List[BigQueryResource]
    destination_table: BigQueryResource

    @classmethod
    def can_parse(cls, entry: LogEntry) -> bool:
        try:
            assert entry.resource.type == "bigquery_dataset"
            assert isinstance(entry.received_timestamp, datetime)
            assert entry.payload["metadata"]["jobChange"]["after"] == "DONE"
            # support only QUERY and COPY job currently, see https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.JobConfig.Type
            assert entry.payload["metadata"]["jobChange"]["job"]["jobConfig"][
                "type"
            ] in ("QUERY", "COPY")
            return True
        except (KeyError, TypeError, AssertionError):
            return False

    @classmethod
    def from_entry(cls, entry: LogEntry) -> "JobChangeEvent":
        timestamp = entry.received_timestamp
        user_email = entry.payload["authenticationInfo"].get("principalEmail", "")

        job = entry.payload["metadata"]["jobChange"]["job"]
        job_name = job.get("jobName")  # Format: projects/<projectId>/jobs/<jobId>

        job_type = job["jobConfig"]["type"]
        query, query_statement_type = None, None

        if job_type == "COPY":
            copy_job = job["jobConfig"]["tableCopyConfig"]
            source_tables = [
                BigQueryResource.from_str(source) for source in copy_job["sourceTables"]
            ]
            destination_table = BigQueryResource.from_str(copy_job["destinationTable"])
        elif job_type == "QUERY":
            query_job = job["jobConfig"]["queryConfig"]
            query = query_job["query"]
            destination_table = BigQueryResource.from_str(query_job["destinationTable"])
            query_statement_type = query_job.get("statementType")

            query_stats = job["jobStats"].get("queryStats", {})
            referenced_tables: List[str] = query_stats.get("referencedTables", [])
            referenced_views: List[str] = query_stats.get("referencedViews", [])
            source_tables = [
                BigQueryResource.from_str(source)
                for source in referenced_tables + referenced_views
            ]
        else:
            raise ValueError(f"unsupported job type {job_type}")

        return cls(
            job_name=job_name,
            timestamp=timestamp,
            user_email=user_email,
            query=query,
            statementType=query_statement_type,
            source_tables=source_tables,
            destination_table=destination_table,
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

        logger.info("Fetching usage and lineage info from BigQuery")

        client = build_client(config)
        self._dataset_filter = config.filter.normalize()

        logger.info("Fetching usage info from tableDataRead log")
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

        logger.info("Fetching lineage info from jobChange log")
        log_filter = self._build_job_change_filter(config, end_time=self._utc_now)
        counter = 0
        for entry in client.list_entries(
            page_size=config.batch_size, filter_=log_filter
        ):
            counter += 1
            if JobChangeEvent.can_parse(entry):
                self._parse_job_change_entry(entry)

        logger.info(f"Number of jobChange log entries fetched: {counter}")

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

    def _parse_job_change_entry(self, entry: LogEntry):
        job_change = JobChangeEvent.from_entry(entry)

        destination = job_change.destination_table
        if not self._dataset_filter.include_schema(
            destination.project_id, destination.dataset_id
        ) or not self._dataset_filter.include_table(
            destination.project_id, destination.dataset_id, destination.table_id
        ):
            logger.info(f"Skipped table: {destination.table_name()}")
            return

        if job_change.user_email in self._excluded_usernames:
            return

        table_name = destination.table_name()
        if table_name not in self._datasets:
            self._datasets[table_name] = Dataset(
                entity_type=EntityType.DATASET,
                logical_id=DatasetLogicalID(
                    name=table_name, platform=DataPlatform.BIGQUERY
                ),
            )

        self._datasets[table_name].upstream = DatasetUpstream(
            source_datasets=[
                str(to_dataset_entity_id(source.table_name(), DataPlatform.BIGQUERY))
                for source in job_change.source_tables
            ],
            transformation=job_change.query,
        )

    @staticmethod
    def _build_job_change_filter(config: BigQueryUsageRunConfig, end_time):
        start = (end_time - timedelta(days=config.lookback_days)).isoformat()
        end = end_time.isoformat()

        return f"""
        resource.type="bigquery_dataset" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.jobChange.after="DONE" AND
        NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult.code:* AND
        protoPayload.metadata.jobChange.job.jobConfig.type=("COPY" OR "QUERY") AND
        timestamp >= "{start}" AND
        timestamp < "{end}"
        """
