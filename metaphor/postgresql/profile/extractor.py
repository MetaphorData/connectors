import asyncio
import traceback
from typing import Collection, List

try:
    import asyncpg
except ImportError:
    print("Please install metaphor[postgresql] extra\n")
    raise

from metaphor.common.column_statistics import ColumnStatistics
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_field_statistics
from metaphor.common.logger import get_logger
from metaphor.common.sampling import SamplingConfig
from metaphor.common.utils import safe_float
from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetFieldStatistics,
    MaterializationType,
)
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.postgresql.profile.config import PostgreSQLProfileRunConfig

logger = get_logger()


class PostgreSQLProfileExtractor(PostgreSQLExtractor):
    """PostgreSQL data profile extractor"""

    _description = "PostgreSQL data profile crawler"

    @staticmethod
    def from_config_file(config_file: str) -> "PostgreSQLProfileExtractor":
        return PostgreSQLProfileExtractor(
            PostgreSQLProfileRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: PostgreSQLProfileRunConfig):
        super().__init__(config)
        self._column_statistics = config.column_statistics
        self._max_concurrency = config.max_concurrency
        self._include_views = config.include_views
        self._sampling = config.sampling

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching data profile from host {self._host}")

        databases = [
            db
            for db in (await self._fetch_databases())
            if self._filter.include_database(db)
        ]
        logger.info(f"Databases to include: {databases}")

        coroutines = [self._profile_database(database) for database in databases]

        await asyncio.gather(*coroutines)

        return [
            dataset
            for dataset in self._datasets.values()
            if self._trim_fields_and_check_empty_dataset(dataset)
        ]

    async def _profile_database(self, database: str) -> None:
        pool = await self._create_connection_pool()

        async with pool.acquire() as conn:
            await self._fetch_tables(conn, database)
            datasets = await self._fetch_columns(conn, database)
            logger.info(f"Include {len(datasets)} datasets from {database}")

        tasks = [
            self._profile_dataset(pool, dataset)
            for dataset in datasets
            if self._filter_dataset_type(dataset)
        ]
        await asyncio.gather(*tasks)
        await pool.close()

    def _filter_dataset_type(self, dataset: Dataset) -> bool:
        """
        Filter out dataset types based on the config, not profile "External", "Stream" and "Snapshot"
        """
        dataset_type = dataset.schema.sql_schema.materialization
        if self._include_views:
            return dataset_type in {
                MaterializationType.TABLE,
                MaterializationType.VIEW,
                MaterializationType.MATERIALIZED_VIEW,
            }
        return dataset_type == MaterializationType.TABLE

    async def _profile_dataset(self, pool: asyncpg.Pool, dataset: Dataset) -> None:
        async with pool.acquire() as conn:
            queries = self._build_profiling_query(
                dataset, self._column_statistics, self._sampling
            )
            try:
                results = []
                for query in queries:
                    res = await conn.fetch(query)
                    assert len(res) == 1
                    results += res[0]
                self._parse_result(results, dataset, self._column_statistics)
            except asyncpg.exceptions.PostgresError as ex:
                logger.error(
                    f"Error when processing {dataset.logical_id.name}, err: {ex}"
                )
                traceback.print_exc()
            dataset.statistics = None
            dataset.schema = None

    @staticmethod
    def _build_profiling_query(
        dataset: Dataset,
        column_statistics: ColumnStatistics,
        sampling: SamplingConfig,
        max_entities_per_query: int = 100,  # more than 400 will get sql compile error
    ) -> List[str]:
        table_name = dataset.logical_id.name

        entities = ["COUNT(1)"]
        for field in dataset.schema.fields:
            column = f'"{field.field_name}"'
            nullable = field.nullable
            is_numeric = field.precision is not None

            if column_statistics.unique_count:
                entities.append(f"COUNT(DISTINCT {column})")

            if nullable and column_statistics.null_count:
                entities.append(f"COUNT(1) - COUNT({column})")

            if is_numeric:
                if column_statistics.min_value:
                    entities.append(f"MIN({column})")
                if column_statistics.max_value:
                    entities.append(f"MAX({column})")
                if column_statistics.avg_value:
                    entities.append(f"AVG({column}::double precision)")

        row_count = dataset.statistics.record_count

        where_clause = ""
        if row_count and sampling.percentage < 100 and row_count >= sampling.threshold:
            logger.info(f"Enable table sampling for table: {table_name}")
            where_clause = f"WHERE random() < 0.{sampling.percentage:02}"

        queries = []
        for start in range(0, len(entities), max_entities_per_query):
            targets = ", ".join(entities[start : start + max_entities_per_query])
            query = f"SELECT {targets} FROM {table_name} {where_clause}".strip()
            queries.append(query)

        return queries

    @staticmethod
    def _parse_result(
        results: List, dataset: Dataset, column_statistics: ColumnStatistics
    ):
        row_count = int(results[0])
        index = 1
        for field in dataset.schema.fields:
            nullable = field.nullable
            is_numeric = field.precision is not None

            unique_values = None
            if column_statistics.unique_count:
                unique_values = safe_float(results[index])
                index += 1

            nulls = 0.0
            if nullable and column_statistics.null_count:
                nulls = float(results[index]) if results[index] else 0.0
                index += 1

            min_value, max_value, avg = None, None, None
            if is_numeric:
                if column_statistics.min_value:
                    min_value = safe_float(results[index])
                    index += 1
                if column_statistics.max_value:
                    max_value = safe_float(results[index])
                    index += 1
                if column_statistics.avg_value:
                    avg = safe_float(results[index])
                    index += 1

            dataset.field_statistics.field_statistics.append(
                build_field_statistics(
                    field.field_path,
                    unique_values,
                    nulls,
                    row_count - nulls,
                    min_value,
                    max_value,
                    avg,
                )
            )

        assert index == len(results)

    async def _create_connection_pool(self) -> asyncpg.Pool:
        logger.info(f"Connecting to DB {self._database}")
        return await asyncpg.create_pool(
            min_size=1,
            max_size=self._max_concurrency,
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
        )

    def _init_dataset(
        self,
        database: str,
        schema: str,
        table: str,
        table_type: str,
        description: str,
        row_count: int,
        table_size: int,
    ) -> None:
        """Overwrite PostgreSQLExtractor._init_dataset"""
        super()._init_dataset(
            database, schema, table, table_type, description, row_count, table_size
        )
        normalized_name = dataset_normalized_name(database, schema, table)
        self._datasets[normalized_name].field_statistics = DatasetFieldStatistics(
            field_statistics=[]
        )

    @staticmethod
    def _trim_fields_and_check_empty_dataset(dataset: Dataset) -> bool:
        """Drop temporary fields and check if the dataset field statistic is empty"""
        if (
            not dataset.field_statistics
            or not dataset.field_statistics.field_statistics
        ):
            return False

        dataset.schema = None
        dataset.statistics = None
        return True
