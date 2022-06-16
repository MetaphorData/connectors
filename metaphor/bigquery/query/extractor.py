import logging
from datetime import timedelta
from typing import Collection, List, Set

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetQueryHistory,
    QueryInfo,
)

from metaphor.bigquery.logEvent import JobChangeEvent
from metaphor.bigquery.query.config import BigQueryQueryRunConfig
from metaphor.bigquery.utils import LogEntry, build_logging_client
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.query_history import TableQueryHistoryHeap
from metaphor.common.utils import start_of_day

logger = get_logger(__name__)
logger.setLevel(logging.INFO)


class BigQueryQueryExtractor(BaseExtractor):
    """BigQuery query history extractor"""

    @staticmethod
    def config_class():
        return BigQueryQueryRunConfig

    def __init__(self):
        self._utc_now = start_of_day()
        self._dataset_filter: DatasetFilter = DatasetFilter()
        self._max_queries_per_table = 0
        self._excluded_usernames: Set[str] = set()

    async def extract(self, config: BigQueryQueryRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, BigQueryQueryExtractor.config_class())

        logger.info("Fetching query history from BigQuery audit log")

        client = build_logging_client(config.key_path, config.project_id)
        self._dataset_filter = config.filter.normalize()
        self._excluded_usernames = config.excluded_usernames
        self._table_queries = TableQueryHistoryHeap(config.max_queries_per_table)

        log_filter = self._build_job_change_filter(config, end_time=self._utc_now)
        counter = 0
        for entry in client.list_entries(
            page_size=config.batch_size, filter_=log_filter
        ):
            counter += 1
            if JobChangeEvent.can_parse(entry):
                self._parse_job_change_entry(entry)

            if counter % 1000 == 0:
                logger.info(f"Fetched {counter} audit logs")

        logger.info(f"Number of queryConfig log entries fetched: {counter}")

        return [
            self._init_dataset(table_name, recent_queries)
            for table_name, recent_queries in self._table_queries.recent_queries()
        ]

    def _parse_job_change_entry(self, entry: LogEntry) -> None:
        job_change = JobChangeEvent.from_entry(entry)
        if job_change is None or job_change.query is None:
            return

        if job_change.user_email in self._excluded_usernames:
            logger.info(f"Skipped query issued by {job_change.user_email}")
            return

        for queried_table in job_change.source_tables:
            if not self._dataset_filter.include_schema(
                queried_table.project_id, queried_table.dataset_id
            ) or not self._dataset_filter.include_table(
                queried_table.project_id,
                queried_table.dataset_id,
                queried_table.table_id,
            ):
                logger.info(f"Skipped table: {queried_table.table_name()}")
                return

            table_name = queried_table.table_name()

            self._table_queries.store_recent_query(
                table_name,
                job_change.timestamp,
                job_change.query,
                job_change.user_email,
            )

    def _init_dataset(
        self, table_name: str, recent_queries: List[QueryInfo]
    ) -> Dataset:
        return Dataset(
            logical_id=DatasetLogicalID(
                name=table_name, platform=DataPlatform.BIGQUERY
            ),
            query_history=DatasetQueryHistory(recent_queries=recent_queries),
        )

    @staticmethod
    def _build_job_change_filter(config: BigQueryQueryRunConfig, end_time) -> str:
        start = (end_time - timedelta(days=config.lookback_days)).isoformat()
        end = end_time.isoformat()

        # Filter for service account
        service_account_filter = (
            "NOT protoPayload.authenticationInfo.principalEmail:gserviceaccount.com AND"
            if config.exclude_service_accounts
            else ""
        )

        # See https://cloud.google.com/logging/docs/view/logging-query-language for query syntax
        return f"""
        resource.type="bigquery_project" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.jobChange.after="DONE" AND
        NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult.code:* AND
        protoPayload.metadata.jobChange.job.jobConfig.type="QUERY" AND
        { service_account_filter }
        timestamp >= "{start}" AND
        timestamp < "{end}"
        """
