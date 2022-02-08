from typing import Iterable, List

from black import asyncio
from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetFieldStatistics,
    FieldStatistics,
    MaterializationType,
    MetadataChangeEvent,
)

from metaphor.common.event_util import EventUtil
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.sampling import SamplingConfig
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.postgresql.profile.config import PostgreSQLProfileRunConfig

try:
    import asyncpg
except ImportError:
    print("Please install metaphor[postgresql] extra\n")
    raise

logger = get_logger(__name__)


class PostgreSQLProfileExtractor(PostgreSQLExtractor):
    """PostgreSQL data profile extractor"""

    @staticmethod
    def config_class():
        return PostgreSQLProfileRunConfig

    async def extract(
        self, config: PostgreSQLProfileRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching data profile from PostgreSQL host {config.host}")

        return await self._extract(config)

    async def _extract(
        self, config: PostgreSQLProfileRunConfig
    ) -> List[MetadataChangeEvent]:

        self._include_views = config.include_views
        self._sampling = config.sampling

        dataset_filter = config.filter.normalize()

        databases = (
            await self._fetch_databases(config)
            if dataset_filter.includes is None
            else list(dataset_filter.includes.keys())
        )

        coroutines = [
            self._profile_database(database, config, dataset_filter)
            for database in databases
        ]

        await asyncio.gather(*coroutines)

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    async def _profile_database(
        self,
        database: str,
        config: PostgreSQLProfileRunConfig,
        dataset_filter: DatasetFilter,
    ) -> None:
        pool = await self._create_connection_pool(config, database)

        async with pool.acquire() as conn:
            await self._fetch_tables(conn, database, dataset_filter)
            datasets = await self._fetch_columns(conn, database, dataset_filter)
            logger.info(f"Include {len(datasets)} tables from {database}")

        tasks = [
            self._profile_dataset(pool, dataset)
            for dataset in datasets
            if dataset.schema.sql_schema.materialization != MaterializationType.VIEW
            or not config.include_views
        ]
        await asyncio.gather(*tasks)
        await pool.close()

        self._trim_fields(datasets)

    async def _profile_dataset(self, pool: asyncpg.Pool, dataset: Dataset) -> None:
        async with pool.acquire() as conn:
            sql = self._build_profiling_query(dataset, self._sampling)
            res = await conn.fetch(sql)
            assert len(res) == 1
            self._parse_result(res[0], dataset)
            dataset.statistics = None
            dataset.schema = None

    @staticmethod
    def _build_profiling_query(
        dataset: Dataset,
        sampling: SamplingConfig,
    ) -> str:

        table_name = dataset.logical_id.name

        query = ["SELECT COUNT(1)"]
        for field in dataset.schema.fields:
            column = f'"{field.field_path}"'
            nullable = field.nullable
            is_numeric = field.precision is not None

            query.append(f", COUNT(DISTINCT {column})")

            if nullable:
                query.append(f", COUNT({column})")

            if is_numeric:
                query.extend(
                    [
                        f", MIN({column})",
                        f", MAX({column})",
                        f", AVG({column})",
                    ]
                )

        query.append(f" FROM {table_name}")

        row_count = dataset.statistics.record_count

        if row_count and sampling.percentage < 100 and row_count >= sampling.threshold:
            logger.info(f"Enable table sampling for table: {table_name}")
            query.append(f" WHERE random() < 0.{sampling.percentage:02}")

        return "".join(query)

    @staticmethod
    def _parse_result(results: List, dataset: Dataset):
        row_count = int(results[0])
        index = 1
        for field in dataset.schema.fields:
            column = field.field_path
            nullable = field.nullable
            is_numeric = field.precision is not None

            unique_values = float(results[index])
            index += 1

            if nullable:
                nulls = float(results[index]) if results[index] else 0.0
                index += 1
            else:
                nulls = 0.0

            if is_numeric:
                min_value = float(results[index]) if results[index] else None
                index += 1
                max_value = float(results[index]) if results[index] else None
                index += 1
                avg = float(results[index]) if results[index] else None
                index += 1
            else:
                min_value, max_value, avg = None, None, None

            dataset.field_statistics.field_statistics.append(
                FieldStatistics(
                    field_path=column,
                    distinct_value_count=unique_values,
                    null_value_count=nulls,
                    nonnull_value_count=(row_count - nulls),
                    min_value=min_value,
                    max_value=max_value,
                    average=avg,
                )
            )

        assert index == len(results)

    @staticmethod
    async def _create_connection_pool(
        config: PostgreSQLProfileRunConfig, database: str
    ) -> asyncpg.Pool:
        logger.info(f"Connecting to DB {database}")
        return await asyncpg.create_pool(
            min_size=1,
            max_size=config.max_concurrency,
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=database,
        )

    def _init_dataset(
        self,
        full_name: str,
        table_type: str,
        description: str,
        row_count: int,
        table_size: int,
    ) -> None:
        """Overwrite PostgreSQLExtractor._init_dataset"""
        super()._init_dataset(full_name, table_type, description, row_count, table_size)
        self._datasets[full_name].field_statistics = DatasetFieldStatistics(
            field_statistics=[]
        )

    @staticmethod
    def _trim_fields(datasets: Iterable[Dataset]) -> None:
        """Drop temporary fields"""
        for dataset in datasets:
            dataset.schema = None
            dataset.statistics = None
