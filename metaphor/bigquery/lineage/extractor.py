import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUpstream,
    EntityType,
    MetadataChangeEvent,
)

from metaphor.bigquery.lineage.config import BigQueryLineageRunConfig
from metaphor.bigquery.utils import BigQueryResource, LogEntry, build_logging_client
from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger

logger = get_logger(__name__)
logger.setLevel(logging.INFO)


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
            assert entry.resource.type == "bigquery_project"
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
    def from_entry(cls, entry: LogEntry) -> Optional["JobChangeEvent"]:
        timestamp = entry.received_timestamp
        user_email = entry.payload["authenticationInfo"].get("principalEmail", "")

        job = entry.payload["metadata"]["jobChange"]["job"]
        job_name = job.get("jobName")  # Format: projects/<projectId>/jobs/<jobId>

        job_type = job["jobConfig"]["type"]
        query, query_statement_type = None, None

        if job_type == "COPY":
            copy_job = job["jobConfig"]["tableCopyConfig"]
            source_tables = [
                BigQueryResource.from_str(source).remove_extras()
                for source in copy_job["sourceTables"]
            ]
            destination_table = BigQueryResource.from_str(
                copy_job["destinationTable"]
            ).remove_extras()
        elif job_type == "QUERY":
            query_job = job["jobConfig"]["queryConfig"]
            query = query_job["query"]
            destination_table = BigQueryResource.from_str(
                query_job["destinationTable"]
            ).remove_extras()
            query_statement_type = query_job.get("statementType")

            query_stats = job["jobStats"].get("queryStats", {})
            referenced_tables: List[str] = query_stats.get("referencedTables", [])
            referenced_views: List[str] = query_stats.get("referencedViews", [])
            source_tables = [
                BigQueryResource.from_str(source).remove_extras()
                for source in referenced_tables + referenced_views
            ]
        else:
            logger.error(f"unsupported job type {job_type}")
            return None

        if destination_table.is_temporary() or any(
            [s.is_temporary() for s in source_tables]
        ):
            return None

        # remove duplicates in source datasets and self referencing
        source_table_set = dict.fromkeys(source_tables)
        source_table_set.pop(destination_table, None)
        if len(source_table_set) == 0:
            return None

        return cls(
            job_name=job_name,
            timestamp=timestamp,
            user_email=user_email,
            query=query,
            statementType=query_statement_type,
            source_tables=list(source_table_set),
            destination_table=destination_table,
        )


class BigQueryLineageExtractor(BaseExtractor):
    """BigQuery lineage metadata extractor"""

    @staticmethod
    def config_class():
        return BigQueryLineageRunConfig

    def __init__(self):
        self._utc_now = datetime.now().replace(tzinfo=timezone.utc)
        self._datasets: Dict[str, Dataset] = {}
        self._datasets_pattern: List[re.Pattern[str]] = []
        self._dataset_filter: DatasetFilter = DatasetFilter()

    async def extract(
        self, config: BigQueryLineageRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, BigQueryLineageExtractor.config_class())

        logger.info("Fetching lineage info from BigQuery")

        client = build_logging_client(config.key_path, config.project_id)
        self._dataset_filter = config.filter.normalize()

        log_filter = self._build_job_change_filter(config, end_time=self._utc_now)
        fetched, parsed = 0, 0
        for entry in client.list_entries(
            page_size=config.batch_size, filter_=log_filter
        ):
            fetched += 1
            if JobChangeEvent.can_parse(entry):
                try:
                    self._parse_job_change_entry(entry)
                    parsed += 1
                except Exception as ex:
                    logger.error(ex)

        logger.info(f"Fetched {fetched} jobChange log entries, parsed {parsed}")

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _parse_job_change_entry(self, entry: LogEntry):
        job_change = JobChangeEvent.from_entry(entry)
        if job_change is None:
            return

        destination = job_change.destination_table
        if not self._dataset_filter.include_schema(
            destination.project_id, destination.dataset_id
        ) or not self._dataset_filter.include_table(
            destination.project_id, destination.dataset_id, destination.table_id
        ):
            logger.info(f"Skipped table: {destination.table_name()}")
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
    def _build_job_change_filter(config: BigQueryLineageRunConfig, end_time):
        start = (end_time - timedelta(days=config.lookback_days)).isoformat()
        end = end_time.isoformat()

        return f"""
        resource.type="bigquery_project" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.jobChange.after="DONE" AND
        NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult.code:* AND
        protoPayload.metadata.jobChange.job.jobConfig.type=("COPY" OR "QUERY") AND
        timestamp >= "{start}" AND
        timestamp < "{end}"
        """
