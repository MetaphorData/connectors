import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from google.cloud._helpers import _rfc3339_nanos_to_datetime

from metaphor.bigquery.utils import BigQueryResource, LogEntry
from metaphor.common.logger import get_logger
from metaphor.common.utils import safe_int, unique_list

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
