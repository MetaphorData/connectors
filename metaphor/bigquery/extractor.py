import time
from concurrent.futures import ThreadPoolExecutor
from typing import Collection, Dict, Iterable, Iterator, List

from metaphor.bigquery.log_filter import build_list_entries_filter
from metaphor.bigquery.queries import Queries
from metaphor.bigquery.table import TableExtractor
from metaphor.common.sql.table_level_lineage import table_level_lineage
from metaphor.snowflake.utils import queried_dataset_entity_id

try:
    import google.cloud.bigquery as bigquery
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from google.cloud.bigquery.table import Table as BQTable

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.bigquery.job_change_event import JobChangeEvent
from metaphor.bigquery.utils import build_client, build_logging_client, get_credentials
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.tag_matcher import tag_datasets
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetStructure,
    EntityUpstream,
    QueryLog,
    SourceInfo,
)

logger = get_logger()


class BigQueryExtractor(BaseExtractor):
    """BigQuery metadata extractor"""

    _description = "BigQuery metadata crawler"
    _platform = Platform.BIGQUERY

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryExtractor":
        return BigQueryExtractor(BigQueryRunConfig.from_yaml_file(config_file))

    def __init__(self, config: BigQueryRunConfig) -> None:
        super().__init__(config)
        self._config = config
        self._credentials = get_credentials(config)
        self._dataset_filter = config.filter.normalize()
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        for project_id in self._config.project_ids:
            self._extract_project(project_id)

        return self._datasets.values()

    def collect_query_logs(self) -> Iterator[QueryLog]:
        for project_id in self._config.project_ids:
            yield from self._fetch_query_logs(project_id)

    def _extract_project(self, project_id: str) -> None:
        logger.info(f"Fetching metadata from BigQuery project {project_id}")

        client = build_client(project_id, self._credentials)

        fetched_tables: List[Dataset] = []
        for dataset_ref in BigQueryExtractor._list_datasets_with_filter(
            client, self._dataset_filter
        ):
            logger.info(f"Fetching tables for {dataset_ref}")

            with ThreadPoolExecutor(
                max_workers=self._config.max_concurrency
            ) as executor:

                def table_ref_to_dataset(table: bigquery.TableReference) -> Dataset:
                    logger.info(f"Getting table {table.table_id}")
                    return self._parse_table(client.project, client.get_table(table))

                # map of table name to Dataset
                tables: Dict[str, Dataset] = {
                    d.logical_id.name.split(".")[-1]: d
                    for d in executor.map(
                        table_ref_to_dataset,
                        BigQueryExtractor._list_tables_with_filter(
                            dataset_ref, client, self._dataset_filter
                        ),
                    )
                    if d.logical_id and d.logical_id.name
                }

            logger.info(f"Getting table DDL for {dataset_ref}")
            table_ddl = client.query(
                Queries.dll(db=dataset_ref.project, schema=dataset_ref.dataset_id),
                project=self._config.job_project_id or self._credentials.project_id,
            ).result()

            for table_name, ddl in table_ddl:
                table = tables.get(str(table_name).lower())
                if table is None:
                    logger.error(f"table {table_name} not found for DDL")
                    continue
                assert table.schema and table.schema.sql_schema
                table.schema.sql_schema.table_schema = ddl

            fetched_tables.extend(tables.values())

        logger.info("Fetching BigQueryAuditMetadata")

        tag_datasets(fetched_tables, self._config.tag_matchers)

        self._datasets.update(
            {
                table.logical_id.name: table
                for table in fetched_tables
                if table.logical_id and table.logical_id.name
            }
        )

        if self._config.lineage.enable_view_lineage:
            self._fetch_view_upstream(client, project_id)

        if self._config.lineage.enable_lineage_from_log:
            self._fetch_audit_log(project_id)

    @staticmethod
    def _list_datasets_with_filter(
        client: bigquery.Client, dataset_filter: DatasetFilter
    ) -> Iterable[bigquery.DatasetReference]:
        for bq_dataset in client.list_datasets():
            if not dataset_filter.include_schema(client.project, bq_dataset.dataset_id):
                logger.info(f"Skipped dataset {bq_dataset.dataset_id}")
                continue

            dataset_ref = bigquery.DatasetReference(
                client.project, bq_dataset.dataset_id
            )
            yield dataset_ref

    @staticmethod
    def _list_tables_with_filter(
        dataset_ref: bigquery.DatasetReference,
        client: bigquery.Client,
        dataset_filter: DatasetFilter,
    ) -> Iterable[bigquery.TableReference]:
        for bq_table in client.list_tables(dataset_ref.dataset_id):
            table_ref = dataset_ref.table(bq_table.table_id)
            if not dataset_filter.include_table(
                client.project, dataset_ref.dataset_id, bq_table.table_id
            ):
                logger.info(f"Skipped table: {table_ref}")
                continue
            yield table_ref

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html#google.cloud.bigquery.table.Table
    def _parse_table(self, project_id: str, bq_table: BQTable) -> Dataset:
        table_name = dataset_normalized_name(
            db=project_id, schema=bq_table.dataset_id, table=bq_table.table_id
        )
        dataset = self._init_dataset(table_name)
        dataset.schema = TableExtractor.extract_schema(bq_table)
        dataset.statistics = to_dataset_statistics(
            bq_table.num_rows, bq_table.num_bytes
        )
        dataset.structure = DatasetStructure(
            database=project_id, schema=bq_table.dataset_id, table=bq_table.table_id
        )
        dataset.source_info = SourceInfo(
            created_at_source=bq_table.created, last_updated=bq_table.modified
        )
        return dataset

    def _fetch_query_logs(self, project_id: str) -> Iterator[QueryLog]:
        client = build_client(project_id, self._credentials)
        logging_client = build_logging_client(project_id, self._credentials)

        fetched, count = 0, 0
        last_time = time.time()

        for entry in logging_client.list_entries(
            page_size=self._config.query_log.fetch_size,
            filter_=build_list_entries_filter("query_log", self._config),
        ):
            count += 1

            if job_change := JobChangeEvent.from_entry(entry):
                if log := job_change.to_query_log(client, self._config):
                    fetched += 1
                    yield log

            if count % self._config.query_log.fetch_size == 0:
                current_time = time.time()
                elapsed_time = current_time - last_time
                wait_time = (
                    60 / self._config.query_log.max_requests_per_minute
                ) - elapsed_time
                last_time = current_time
                if wait_time > 0:
                    time.sleep(wait_time)

        logger.info(f"Number of query log entries fetched: {fetched}")

    def _fetch_view_upstream(self, client: bigquery.Client, project_id: str) -> None:
        logger.info("Fetching lineage info from BigQuery API")

        for dataset_ref in self._list_datasets_with_filter(
            client, self._dataset_filter
        ):
            logger.info(f"Found dataset {dataset_ref}")
            for table_ref in self._list_tables_with_filter(
                dataset_ref, client, self._dataset_filter
            ):
                logger.debug(f"Found table {table_ref}")
                try:
                    bq_table = client.get_table(table_ref)
                    self._parse_view_lineage(project_id, bq_table)
                except Exception as ex:
                    logger.exception(ex)

    def _parse_view_lineage(self, project_id: str, bq_table: BQTable) -> None:
        view_query = bq_table.view_query or bq_table.mview_query
        if not view_query:
            return

        view_name = dataset_normalized_name(
            db=project_id, schema=bq_table.dataset_id, table=bq_table.table_id
        )
        logger.info(f"Found view {view_name}")

        sources = [
            queried_dataset_entity_id(source, None)
            for source in table_level_lineage.extract_table_level_lineage(
                view_query, DataPlatform.BIGQUERY, None
            ).sources
        ]
        if sources:
            dataset = self._init_dataset(view_name)
            dataset.entity_upstream = EntityUpstream(
                source_entities=list(sources), transformation=view_query
            )

    def _init_dataset(self, table_name: str) -> Dataset:
        if table_name not in self._datasets:
            self._datasets[table_name] = Dataset(
                logical_id=DatasetLogicalID(
                    name=table_name, platform=DataPlatform.BIGQUERY
                ),
            )

        return self._datasets[table_name]

    def _fetch_audit_log(self, project_id: str):
        logger.info("Fetching lineage info from BigQuery Audit log")

        logging_client = build_logging_client(project_id, self._credentials)

        fetched, parsed = 0, 0
        for entry in logging_client.list_entries(
            page_size=self._config.lineage.batch_size,
            filter_=build_list_entries_filter("audit_log", self._config),
        ):
            fetched += 1
            try:
                if job_change := JobChangeEvent.from_entry(entry):
                    self._extract_entity_upstream_from_job_change(job_change)
                    parsed += 1
            except Exception as ex:
                logger.exception(ex)

            if fetched % 1000 == 0:
                logger.info(f"Fetched {fetched} audit logs")

        logger.info(f"Fetched {fetched} jobChange log entries, parsed {parsed}")

    def _extract_entity_upstream_from_job_change(self, job_change: JobChangeEvent):
        if job_change.destination_table is None:
            return

        # Filter out self lineage
        if not self._config.lineage.include_self_lineage:
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
