import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from metaphor.bigquery.utils import BigQueryResource, LogEntry
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list

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

        # remove temporary and information schema tables, and duplicates in sources
        if (
            destination_table.is_temporary()
            or destination_table.is_information_schema()
        ):
            return None

        source_tables = unique_list(
            [
                t
                for t in source_tables
                if not t.is_temporary() and not t.is_information_schema()
            ]
        )

        if len(source_tables) == 0:
            return None

        return cls(
            job_name=job_name,
            timestamp=timestamp,
            user_email=user_email,
            query=query,
            statementType=query_statement_type,
            source_tables=source_tables,
            destination_table=destination_table,
        )
