import asyncio
import traceback
from typing import Collection, Iterable, List

try:
    import asyncpg
except ImportError:
    print("Please install metaphor[postgresql] extra\n")
    raise

from metaphor.common.column_statistics import ColumnStatistics
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.sampling import SamplingConfig
from metaphor.common.utils import safe_float
from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetFieldStatistics,
    FieldStatistics,
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

        databases = (
            await self._fetch_databases()
            if self._filter.includes is None
            else list(self._filter.includes.keys())
        )

        coroutines = [
            self._profile_database(database)
            for database in databases
            if self._filter.include_database(database)
        ]

        await asyncio.gather(*coroutines)

        return self._datasets.values()

    async def _profile_database(self, database: str) -> None:
        pool = await self._create_connection_pool()

        async with pool.acquire() as conn:
            await self._fetch_tables(conn, database)
            datasets = await self._fetch_columns(conn, database)
            logger.info(f"Include {len(datasets)} tables from {database}")

        tasks = [
            self._profile_dataset(pool, dataset)
            for dataset in datasets
            if dataset.schema.sql_schema.materialization != MaterializationType.VIEW
            or not self._include_views
        ]
        await asyncio.gather(*tasks)
        await pool.close()

        self._trim_fields(datasets)

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
                FieldStatistics(
                    field_path=field.field_path,
                    distinct_value_count=unique_values,
                    null_value_count=nulls,
                    nonnull_value_count=(row_count - nulls),
                    min_value=min_value,
                    max_value=max_value,
                    average=avg,
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
    def _trim_fields(datasets: Iterable[Dataset]) -> None:
        """Drop temporary fields"""
        for dataset in datasets:
            dataset.schema = None
            dataset.statistics = None
