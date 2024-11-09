import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from google.cloud import bigquery
from google.cloud._helpers import _rfc3339_nanos_to_datetime

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.bigquery.log_type import query_type_to_log_type
from metaphor.bigquery.utils import BigQueryResource, LogEntry
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.logger import get_logger
from metaphor.common.sql.query_log import PartialQueryLog, process_and_init_query_log
from metaphor.common.utils import safe_float, safe_int, unique_list
from metaphor.models.metadata_change_event import DataPlatform, QueriedDataset, QueryLog

logger = get_logger()
logger.setLevel(logging.INFO)


@dataclass
class JobChangeEvent:
    """
    Container class for BigQueryAuditMetadata.JobChange, where the 'after' job status is 'DONE'
    See https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.jobchange
    """

    job_name: str
    job_type: str
    timestamp: datetime
    start_time: Optional[datetime]  # Job execution start time.
    end_time: Optional[datetime]  # Job completion time.
    user_email: str

    query: Optional[str]
    query_truncated: Optional[bool]
    statementType: Optional[str]
    source_tables: List[BigQueryResource]
    destination_table: Optional[BigQueryResource]
    default_dataset: Optional[str]

    input_bytes: Optional[int]
    output_bytes: Optional[int]
    output_rows: Optional[int]

    @staticmethod
    def _can_parse(entry: LogEntry) -> bool:
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
        if not JobChangeEvent._can_parse(entry):
            return None
        timestamp = entry.received_timestamp
        user_email = entry.payload["authenticationInfo"].get("principalEmail", "")

        job = entry.payload["metadata"]["jobChange"]["job"]
        job_name = job.get("jobName")  # Format: projects/<projectId>/jobs/<jobId>

        job_type = job["jobConfig"]["type"]

        query, query_statement_type = None, None
        query_truncated = None
        destination_table = None
        default_dataset = None

        input_bytes, output_bytes, output_rows = None, None, None

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
            query_truncated = query_job.get("queryTruncated", None)

            # Not all query jobs will have a destination table, e.g. calling a stored procedure
            if "destinationTable" in query_job:
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
            default_dataset = query_job.get("defaultDataset", None)

            processed_bytes = query_stats.get("totalProcessedBytes", None)
            input_bytes = safe_int(processed_bytes)

            output_row_count = query_stats.get("outputRowCount", None)
            output_rows = safe_int(output_row_count)
        else:
            logger.error(f"unsupported job type {job_type}")
            return None

        # remove temporary and information schema tables, and duplicates in sources
        if destination_table and (
            destination_table.is_temporary()
            or destination_table.is_information_schema()
        ):
            destination_table = None

        source_tables = unique_list(
            [
                t
                for t in source_tables
                if not t.is_temporary() and not t.is_information_schema()
            ]
        )

        jobStats = job["jobStats"]
        start_time = _rfc3339_nanos_to_datetime(jobStats["startTime"])
        end_time = _rfc3339_nanos_to_datetime(jobStats["endTime"])

        if len(source_tables) == 0:
            return None

        return cls(
            job_name=job_name,
            job_type=job_type,
            timestamp=timestamp,
            user_email=user_email,
            query=query,
            query_truncated=query_truncated,
            statementType=query_statement_type,
            source_tables=source_tables,
            destination_table=destination_table,
            default_dataset=default_dataset,
            start_time=start_time,
            end_time=end_time,
            input_bytes=input_bytes,
            output_bytes=output_bytes,
            output_rows=output_rows,
        )

    def _get_email(self, service_account: bool) -> Optional[str]:
        """
        Returns the email for this Job Change Event.

        Paramaters
        ----------
        service_account : bool
            If `service_account` is set to `True`, only returns the email if this email
            belongs to a service account, otherwise returns `None`.
            If `service_account` is set to `False`, only returns the email if this email
            does not belong to a service account.
        """
        # Assume service accounts always end in ".gserviceaccount.com"
        # https://cloud.google.com/compute/docs/access/service-accounts
        is_service_account = self.user_email.endswith(".gserviceaccount.com")
        if service_account:
            return self.user_email if is_service_account else None
        else:
            return None if is_service_account else self.user_email

    def to_query_log(
        self, client: bigquery.Client, config: BigQueryRunConfig
    ) -> Optional[QueryLog]:
        """
        Converts this JobChangeEvent to a QueryLog.
        """
        if self.query is None:
            return None

        if self.user_email in config.query_log.excluded_usernames:
            logger.debug(f"Skipped query issued by {self.user_email}")
            return None

        sources: List[QueriedDataset] = [
            self._convert_resource_to_queried_dataset(d) for d in self.source_tables
        ]
        target = self.destination_table
        target_datasets = (
            [self._convert_resource_to_queried_dataset(target)] if target else None
        )

        default_database, default_schema = None, None
        if self.default_dataset and self.default_dataset.count(".") == 1:
            default_database, default_schema = self.default_dataset.split(".")

        query = self.query
        # if query SQL is truncated, fetch full SQL from job API
        if (
            self.job_type == "QUERY"
            and self.query_truncated
            and config.query_log.fetch_job_query_if_truncated
        ):
            query = self._fetch_job_query(client, self.job_name) or query

        elapsed_time = (
            (self.end_time - self.start_time).total_seconds()
            if self.start_time and self.end_time
            else None
        )

        return process_and_init_query_log(
            query=query,
            platform=DataPlatform.BIGQUERY,
            process_query_config=config.query_log.process_query,
            query_log=PartialQueryLog(
                start_time=self.start_time,
                duration=safe_float(elapsed_time),
                user_id=self._get_email(service_account=True),
                email=self._get_email(service_account=False),
                rows_written=safe_float(self.output_rows),
                bytes_read=safe_float(self.input_bytes),
                bytes_written=safe_float(self.output_bytes),
                sources=sources,
                targets=target_datasets,
                default_database=default_database,
                default_schema=default_schema,
                type=query_type_to_log_type(self.statementType),
            ),
            query_id=self.job_name,
        )

    @staticmethod
    def _fetch_job_query(client: bigquery.Client, job_name: str) -> Optional[str]:
        logger.info(f"Query {job_name}")
        if match := re.match(r"^projects/([^/]+)/jobs/([^/]+)$", job_name):
            project = match.group(1)
            job_id = match.group(2)

            try:
                job = client.get_job(job_id, project)
            except Exception as e:
                logger.warning(f"Failed to get job information: {e}")
                return None

            if isinstance(job, bigquery.QueryJob):
                return job.query

        return None

    @staticmethod
    def _convert_resource_to_queried_dataset(
        resource: BigQueryResource,
    ) -> QueriedDataset:
        dataset_name = dataset_normalized_name(
            resource.project_id, resource.dataset_id, resource.table_id
        )
        dataset_id = str(to_dataset_entity_id(dataset_name, DataPlatform.BIGQUERY))
        return QueriedDataset(
            id=dataset_id,
            database=resource.project_id,
            schema=resource.dataset_id,
            table=resource.table_id,
        )
