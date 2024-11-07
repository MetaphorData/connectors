from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import sleep
from typing import Collection, List, Set, Union

try:
    import google.cloud.bigquery as bigquery
    from google.cloud.bigquery import QueryJob, TableReference
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from metaphor.bigquery.profile.config import BigQueryProfileRunConfig, SamplingConfig
from metaphor.bigquery.table import TableExtractor
from metaphor.bigquery.utils import build_client, get_credentials
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.column_statistics import ColumnStatistics
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_field_statistics
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.utils import safe_float
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    DatasetSchema,
)

logger = get_logger()


class BigQueryProfileExtractor(BaseExtractor):
    """BigQuery data profile extractor"""

    _description = "BigQuery data profile crawler"
    _platform = Platform.BIGQUERY

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryProfileExtractor":
        return BigQueryProfileExtractor(
            BigQueryProfileRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: BigQueryProfileRunConfig):
        super().__init__(config)
        self._config = config
        self._credentials = get_credentials(config)
        self._project_ids = config.project_ids
        self._job_project_id = config.job_project_id or self._credentials.project_id
        self._max_concurrency = config.max_concurrency
        self._filter = DatasetFilter.normalize(config.filter)
        self._column_statistics = config.column_statistics
        self._sampling = config.sampling
        self._datasets: List[Dataset] = []

    async def extract(self) -> Collection[ENTITY_TYPES]:
        for project_id in self._project_ids:
            self._extract_project(project_id)

        return self._datasets

    def _extract_project(self, project_id: str):
        logger.info(f"Fetching usage info from BigQuery for project {project_id}")

        self._client = build_client(project_id, self._credentials)
        tables = self._fetch_tables()
        self.profile(tables)

    def _fetch_tables(self) -> List[TableReference]:
        tables = []
        for bq_dataset in self._client.list_datasets():
            dataset_ref = bigquery.DatasetReference(
                self._client.project, bq_dataset.dataset_id
            )

            logger.info(f"Found dataset {dataset_ref}")

            for bq_table in self._client.list_tables(bq_dataset.dataset_id):
                table_ref = dataset_ref.table(bq_table.table_id)
                if not self._filter.include_table(
                    database=table_ref.project,
                    schema=table_ref.dataset_id,
                    table=table_ref.table_id,
                ):
                    logger.info(f"Ignore {table_ref} due to filter config")
                    continue
                logger.info(f"Found table {table_ref}")

                tables.append(table_ref)

        return tables

    def profile(self, tables: List[TableReference]):
        jobs: Set[QueryJob] = set()

        def profile(table: TableReference):
            return self._profile_table(table, jobs)

        with ThreadPoolExecutor(max_workers=self._max_concurrency) as executor:
            datasets = [dataset for dataset in executor.map(profile, tables)]

        counter = 0
        while jobs:
            finished = len(datasets) - len(jobs)
            if counter % 10 == 0:
                logger.info(
                    f"{finished} jobs done, waiting for {len(jobs)} jobs to finish ..."
                )
            counter += 1
            sleep(1)

        self._datasets.extend(datasets)

    def _profile_table(
        self,
        table: TableReference,
        jobs: Set[QueryJob],
    ) -> Dataset:
        def job_callback(job: QueryJob, schema: DatasetSchema, dataset: Dataset):
            exception = job.exception()
            if exception:
                logger.error(f"Skip {table}, Google Client error: {exception}")
            # The profiling result should only have one row
            elif job.result().total_rows != 1:
                logger.warning(
                    f"Skip {table}, the profiling result has more than one row"
                )
            else:
                try:
                    results = [res for res in next(job.result())]
                    BigQueryProfileExtractor._parse_result(
                        results, schema, dataset, self._column_statistics
                    )
                except AssertionError as error:
                    logger.error(f"Assertion failed during process results, {error}")
                except StopIteration as error:
                    logger.error(f"Invalid result, {error}")
                except (IndexError, ValueError, TypeError) as error:
                    logger.error(f"Unknown error during process results, {error}")
                except BaseException as error:
                    jobs.remove(job.job_id)
                    raise error

            # Always remove job from the set, because the job(future) is fulfill
            jobs.remove(job.job_id)

        dataset = BigQueryProfileExtractor._init_dataset(
            dataset_normalized_name(
                db=table.project, schema=table.dataset_id, table=table.table_id
            )
        )
        bq_table = self._client.get_table(table)
        row_count = bq_table.num_rows
        schema = TableExtractor.extract_schema(bq_table)

        logger.debug(f"building query for {table}")
        sql = self._build_profiling_query(
            schema, table, row_count, self._column_statistics, self._sampling
        )
        job = self._client.query(sql, project=self._job_project_id)

        jobs.add(job.job_id)
        job.add_done_callback(partial(job_callback, schema=schema, dataset=dataset))
        logger.info(f"Job dispatched for {table}")
        return dataset

    @staticmethod
    def _build_profiling_query(
        schema: DatasetSchema,
        table_ref: TableReference,
        row_count: Union[int, None],
        column_statistics: ColumnStatistics,
        sampling: SamplingConfig,
    ) -> str:
        query = ["SELECT COUNT(1)"]

        for field in schema.fields:
            column = field.field_name
            data_type = field.native_type

            if (
                column_statistics.unique_count
                and not BigQueryProfileExtractor._is_complex(data_type)
            ):
                query.append(f", COUNT(DISTINCT `{column}`)")

            if column_statistics.null_count:
                query.append(f", COUNTIF(`{column}` is NULL)")

            if BigQueryProfileExtractor._is_numeric(data_type):
                if column_statistics.min_value:
                    query.append(f", MIN(`{column}`)")
                if column_statistics.max_value:
                    query.append(f", MAX(`{column}`)")
                if column_statistics.avg_value:
                    query.append(f", AVG(`{column}`)")
                if column_statistics.std_dev:
                    query.append(f", STDDEV(`{column}`)")

        query.append(f" FROM `{table_ref}`")

        if row_count and sampling.percentage < 100 and row_count >= sampling.threshold:
            logger.info(f"Enable table sampling for table: {table_ref}")
            query.append(f" TABLESAMPLE SYSTEM ({int(sampling.percentage)} PERCENT)")

        logger.debug(query)
        return "".join(query)

    @staticmethod
    def _parse_result(
        results: List,
        schema: DatasetSchema,
        dataset: Dataset,
        column_statistics: ColumnStatistics,
    ):
        assert (
            dataset.field_statistics is not None
            and dataset.field_statistics.field_statistics is not None
        )
        fields = dataset.field_statistics.field_statistics

        assert len(results) > 0, f"Empty result for ${dataset.logical_id}"
        row_count = int(results[0])

        index = 1
        for field in schema.fields:
            data_type = field.native_type

            unique_count = None
            if (
                column_statistics.unique_count
                and not BigQueryProfileExtractor._is_complex(data_type)
            ):
                unique_count = float(results[index])
                index += 1

            nulls, non_nulls = None, None
            if column_statistics.null_count:
                nulls = float(results[index])
                non_nulls = row_count - nulls
                index += 1

            min_value, max_value, avg, std_dev = None, None, None, None
            if BigQueryProfileExtractor._is_numeric(data_type):
                if column_statistics.min_value:
                    min_value = safe_float(results[index])
                    index += 1

                if column_statistics.max_value:
                    max_value = safe_float(results[index])
                    index += 1

                if column_statistics.avg_value:
                    avg = safe_float(results[index])
                    index += 1

                if column_statistics.std_dev:
                    std_dev = safe_float(results[index])
                    index += 1

            fields.append(
                build_field_statistics(
                    field.field_path,
                    unique_count,
                    nulls,
                    non_nulls,
                    min_value,
                    max_value,
                    avg,
                    std_dev,
                )
            )

        # Verify that we've consumed all the elements from results
        assert index == len(
            results
        ), f"Unconsumed elements from results for {dataset.logical_id}"

    # See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types
    @staticmethod
    def _is_numeric(data_type: str) -> bool:
        numeric_types = [
            "INTEGER",
            "NUMERIC",
            "INT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "TINYINT",
            "BYTEINT",
            "DECIMAL",
            "BIGNUMERIC",
            "BIGDECIMAL",
            "FLOAT64",
        ]
        return data_type in numeric_types

    @staticmethod
    def _is_complex(data_type: str) -> bool:
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#groupable_data_types
        return data_type in ("RECORD", "JSON", "GEOGRAPHY") or data_type.startswith(
            "ARRAY"
        )

    @staticmethod
    def _init_dataset(full_name: str) -> Dataset:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=full_name, platform=DataPlatform.BIGQUERY
        )

        dataset.field_statistics = DatasetFieldStatistics(field_statistics=[])

        return dataset
