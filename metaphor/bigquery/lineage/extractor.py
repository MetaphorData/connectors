from datetime import timedelta
from typing import Collection, Dict

try:
    import google.cloud.bigquery as bigquery
    from google.cloud import logging_v2
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from sql_metadata import Parser

from metaphor.bigquery.lineage.config import BigQueryLineageRunConfig
from metaphor.bigquery.logEvent import JobChangeEvent
from metaphor.bigquery.utils import (
    LogEntry,
    build_client,
    build_logging_client,
    get_credentials,
)
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    EntityUpstream,
)

logger = get_logger()


class BigQueryLineageExtractor(BaseExtractor):
    """BigQuery lineage metadata extractor"""

    _description = "BigQuery data lineage crawler"
    _platform = Platform.BIGQUERY

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryLineageExtractor":
        return BigQueryLineageExtractor(
            BigQueryLineageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: BigQueryLineageRunConfig):
        super().__init__(config)
        self._config = config
        self._credentials = get_credentials(config)
        self._project_ids = config.project_ids
        self._dataset_filter = config.filter.normalize()
        self._enable_view_lineage = config.enable_view_lineage
        self._enable_lineage_from_log = config.enable_lineage_from_log
        self._include_self_lineage = config.include_self_lineage
        self._lookback_days = config.lookback_days
        self._batch_size = config.batch_size

        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        for project_id in self._project_ids:
            self._extract_project(project_id)

        return self._datasets.values()

    def _extract_project(self, project_id: str):
        logger.info(f"Extracting lineage for project {project_id}")

        client = build_client(project_id, self._credentials)
        logging_client = build_logging_client(project_id, self._credentials)

        if self._enable_view_lineage:
            self._fetch_view_upstream(client, project_id)

        if self._enable_lineage_from_log:
            self._fetch_audit_log(logging_client)

    def _fetch_view_upstream(self, client: bigquery.Client, project_id: str) -> None:
        logger.info("Fetching lineage info from BigQuery API")

        for bq_dataset in client.list_datasets():
            if not self._dataset_filter.include_schema(
                project_id, bq_dataset.dataset_id
            ):
                logger.info(f"Skipped dataset {bq_dataset.dataset_id}")
                continue

            dataset_ref = bigquery.DatasetReference(
                client.project, bq_dataset.dataset_id
            )

            logger.info(f"Found dataset {dataset_ref}")

            for bq_table in client.list_tables(bq_dataset.dataset_id):
                table_ref = dataset_ref.table(bq_table.table_id)
                if not self._dataset_filter.include_table(
                    project_id, bq_dataset.dataset_id, bq_table.table_id
                ):
                    logger.info(f"Skipped table: {table_ref}")
                    continue
                logger.debug(f"Found table {table_ref}")

                try:
                    bq_table = client.get_table(table_ref)
                    self._parse_view_lineage(client.project, bq_table)
                except Exception as ex:
                    logger.exception(ex)

    def _parse_view_lineage(self, project_id, bq_table: bigquery.table.Table) -> None:
        view_query = bq_table.view_query or bq_table.mview_query
        if not view_query:
            return

        view_name = dataset_normalized_name(
            db=project_id, schema=bq_table.dataset_id, table=bq_table.table_id
        )
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
                        dataset_normalized_name(table=dataset_name),
                        DataPlatform.BIGQUERY,
                        None,
                    )
                )
            )

        if dataset_ids:
            dataset = self._init_dataset(view_name)
            dataset.entity_upstream = EntityUpstream(
                source_entities=list(dataset_ids), transformation=view_query
            )

    def _fetch_audit_log(self, logging_client: logging_v2.Client):
        logger.info("Fetching lineage info from BigQuery Audit log")

        log_filter = self._build_job_change_filter()
        fetched, parsed = 0, 0
        for entry in logging_client.list_entries(
            page_size=self._batch_size, filter_=log_filter
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

        assert job_change.destination_table is not None

        # Filter out self lineage
        if not self._include_self_lineage:
            job_change.source_tables = [
                t for t in job_change.source_tables if t != job_change.destination_table
            ]

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
        source_entities = [
            str(to_dataset_entity_id(source.table_name(), DataPlatform.BIGQUERY))
            for source in job_change.source_tables
        ]
        dataset.entity_upstream = EntityUpstream(
            source_entities=source_entities,
            transformation=job_change.query,
        )

    def _build_job_change_filter(self):
        end_time = start_of_day()
        start = (end_time - timedelta(days=self._lookback_days)).isoformat()
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
                logical_id=DatasetLogicalID(
                    name=table_name, platform=DataPlatform.BIGQUERY
                ),
            )

        return self._datasets[table_name]
