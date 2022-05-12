import json
import logging
from datetime import timedelta
from hashlib import sha256
from typing import Collection, Dict, Set

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
        self._datasets: Dict[str, Dataset] = {}
        self._dataset_filter: DatasetFilter = DatasetFilter()
        self._max_queries_per_table = 0
        self._excluded_usernames: Set[str] = set()
        self._query_hashes: Dict[str, Set[str]] = {}

    async def extract(self, config: BigQueryQueryRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, BigQueryQueryExtractor.config_class())

        logger.info("Fetching query history from BigQuery audit log")

        client = build_logging_client(config.key_path, config.project_id)
        self._dataset_filter = config.filter.normalize()
        self._excluded_usernames = config.excluded_usernames
        self._max_queries_per_table = config.max_queries_per_table

        log_filter = self._build_job_change_filter(config, end_time=self._utc_now)
        counter = 0
        for entry in client.list_entries(
            page_size=config.batch_size, filter_=log_filter
        ):
            counter += 1
            if JobChangeEvent.can_parse(entry):
                print(json.dumps(entry.to_api_repr()))

                self._parse_job_change_entry(entry)

            if counter % 1000 == 0:
                logger.info(f"Fetched {counter} audit logs")

        logger.info(f"Number of queryConfig log entries fetched: {counter}")

        return self._datasets.values()

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
            dataset = self._init_dataset(table_name)

            # Skip identical queries
            hashes = self._query_hashes.setdefault(table_name, set())
            query_hash = sha256(job_change.query.encode("utf8")).hexdigest()
            if query_hash in hashes:
                return

            hashes.add(query_hash)

            query_info = QueryInfo(
                query=job_change.query,
                issued_by=job_change.user_email,
                issued_at=job_change.timestamp,
            )

            # Store recent queries in reverse chronological order by prepending the latest query
            dataset.query_history.recent_queries = [
                query_info
            ] + dataset.query_history.recent_queries[: self._max_queries_per_table - 1]

    def _init_dataset(self, table_name: str) -> Dataset:
        if table_name not in self._datasets:
            self._datasets[table_name] = Dataset(
                logical_id=DatasetLogicalID(
                    name=table_name, platform=DataPlatform.BIGQUERY
                ),
                query_history=DatasetQueryHistory(recent_queries=[]),
            )

        return self._datasets[table_name]

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
