import json
from datetime import datetime, timedelta, timezone
from typing import Collection, Dict, Optional

try:
    import google.cloud.bigquery as bigquery
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUpstream,
    EntityType,
)
from sql_metadata import Parser

from metaphor.bigquery.lineage.config import BigQueryLineageRunConfig
from metaphor.bigquery.logEvent import JobChangeEvent
from metaphor.bigquery.utils import LogEntry, build_client, build_logging_client
from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger

logger = get_logger(__name__)


class BigQueryLineageExtractor(BaseExtractor):
    """BigQuery lineage metadata extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.BIGQUERY

    def description(self) -> str:
        return "BigQuery data lineage crawler"

    @staticmethod
    def config_class():
        return BigQueryLineageRunConfig

    def __init__(self):
        self._utc_now = datetime.now().replace(tzinfo=timezone.utc)
        self._datasets: Dict[str, Dataset] = {}
        self._dataset_filter: DatasetFilter = DatasetFilter()

    async def extract(
        self, config: BigQueryLineageRunConfig
    ) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, BigQueryLineageExtractor.config_class())

        if config.enable_view_lineage:
            self._fetch_view_upstream(config)

        if config.enable_lineage_from_log:
            self._fetch_audit_log(config)

        return self._datasets.values()

    def _fetch_view_upstream(self, config: BigQueryLineageRunConfig) -> None:
        logger.info("Fetching lineage info from BigQuery API")

        client = build_client(config)

        dataset_filter = config.filter.normalize()

        for bq_dataset in client.list_datasets():
            if not dataset_filter.include_schema(
                config.project_id, bq_dataset.dataset_id
            ):
                logger.info(f"Skipped dataset {bq_dataset.dataset_id}")
                continue

            dataset_ref = bigquery.DatasetReference(
                client.project, bq_dataset.dataset_id
            )

            logger.info(f"Found dataset {dataset_ref}")

            for bq_table in client.list_tables(bq_dataset.dataset_id):
                table_ref = dataset_ref.table(bq_table.table_id)
                if not dataset_filter.include_table(
                    config.project_id, bq_dataset.dataset_id, bq_table.table_id
                ):
                    logger.info(f"Skipped table: {table_ref}")
                    continue
                logger.debug(f"Found table {table_ref}")

                bq_table = client.get_table(table_ref)
                try:
                    self._parse_view_lineage(client.project, bq_table)
                except Exception as ex:
                    logger.exception(ex)

    def _parse_view_lineage(self, project_id, bq_table: bigquery.table.Table) -> None:
        view_query = bq_table.view_query or bq_table.mview_query
        if not view_query:
            return

        view_name = f"{project_id}.{bq_table.dataset_id}.{bq_table.table_id}"
        logger.info(f"Found view {view_name}")

        tables = Parser(view_query).tables

        dataset_ids = set()
        for table in tables:
            segments = table.count(".") + 1
            if segments == 3:
                dataset_name = table
            elif segments == 2:
                dataset_name = f"{project_id}.{table}"
            elif segments == 1:
                dataset_name = f"{project_id}.{bq_table.dataset_id}.{table}"
            else:
                raise ValueError(f"invalid table name {table}")

            dataset_ids.add(
                str(
                    to_dataset_entity_id(
                        dataset_name.replace("`", "").lower(),
                        DataPlatform.BIGQUERY,
                        None,
                    )
                )
            )

        if dataset_ids:
            dataset = self._init_dataset(view_name)
            dataset.upstream = DatasetUpstream(
                source_datasets=list(dataset_ids), transformation=view_query
            )

    def _fetch_audit_log(self, config: BigQueryLineageRunConfig):
        logger.info("Fetching lineage info from BigQuery Audit log")

        client = build_logging_client(config)
        self._dataset_filter = config.filter.normalize()

        log_filter = self._build_job_change_filter(config, end_time=self._utc_now)
        fetched, parsed = 0, 0
        for entry in client.list_entries(
            page_size=config.batch_size, filter_=log_filter
        ):
            fetched += 1
            try:
                if JobChangeEvent.can_parse(entry):
                    self._parse_job_change_entry(entry)
                    parsed += 1
            except Exception as ex:
                logger.exception(ex)

            if fetched % 1000 == 0:
                logger.info(f"Fetched {fetched} audit logs")

        logger.info(f"Fetched {fetched} jobChange log entries, parsed {parsed}")

    def _parse_job_change_entry(self, entry: LogEntry):
        job_change = JobChangeEvent.from_entry(entry)
        if job_change is None or job_change.destination_table is None:
            return

        if job_change.destination_table in job_change.source_tables:
            logger.warning(
                f"self referencing tables in job {json.dumps(job_change.__dict__, default=str)}"
            )
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
        dataset = self._init_dataset(table_name)
        dataset.upstream = DatasetUpstream(
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

        # See https://cloud.google.com/logging/docs/view/logging-query-language for query syntax
        return f"""
        resource.type="bigquery_project" AND
        protoPayload.serviceName="bigquery.googleapis.com" AND
        protoPayload.metadata.jobChange.after="DONE" AND
        NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult.code:* AND
        protoPayload.metadata.jobChange.job.jobConfig.type=("COPY" OR "QUERY") AND
        NOT protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable:"/datasets/_" AND
        timestamp >= "{start}" AND
        timestamp < "{end}"
        """

    def _init_dataset(self, table_name: str) -> Dataset:
        if table_name not in self._datasets:
            self._datasets[table_name] = Dataset(
                entity_type=EntityType.DATASET,
                logical_id=DatasetLogicalID(
                    name=table_name, platform=DataPlatform.BIGQUERY
                ),
            )

        return self._datasets[table_name]
