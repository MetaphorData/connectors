import logging
from typing import Collection, Dict, List, Optional, Tuple

from metaphor.models.crawler_run_metadata import Platform

try:
    import snowflake.connector
except ImportError:
    print("Please install metaphor[snowflake] extra\n")
    raise


from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    EntityType,
    FieldStatistics,
)

from metaphor.common.entity_id import dataset_fullname
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.sampling import SamplingConfig
from metaphor.snowflake.auth import connect
from metaphor.snowflake.extractor import SnowflakeExtractor
from metaphor.snowflake.profile.config import (
    ColumnStatistics,
    SnowflakeProfileRunConfig,
)
from metaphor.snowflake.utils import (
    DatasetInfo,
    QueryWithParam,
    SnowflakeTableType,
    async_execute,
)

logger = get_logger(__name__)

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)


class SnowflakeProfileExtractor(BaseExtractor):
    """Snowflake data profile extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.SNOWFLAKE

    def description(self) -> str:
        return "Snowflake data profile crawler"

    @staticmethod
    def config_class():
        return SnowflakeProfileRunConfig

    def __init__(self):
        self.max_concurrency = None
        self.include_views = None
        self._datasets: Dict[str, Dataset] = {}
        self._column_statistics = None
        self._sampling = None

    async def extract(
        self, config: SnowflakeProfileRunConfig
    ) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, SnowflakeProfileExtractor.config_class())

        logger.info("Fetching data profile from Snowflake")
        self.max_concurrency = config.max_concurrency
        self.include_views = config.include_views
        self._column_statistics = config.column_statistics
        self._sampling = config.sampling

        conn = connect(config)

        with conn:
            cursor = conn.cursor()

            filter = config.filter.normalize()

            databases = (
                SnowflakeExtractor.fetch_databases(cursor)
                if filter.includes is None
                else list(filter.includes.keys())
            )

            for database in databases:
                tables = self._fetch_tables(cursor, database, config.account, filter)
                logger.info(f"Include {len(tables)} tables from {database}")

                self._fetch_columns_async(conn, tables)

        logger.debug(self._datasets)

        return self._datasets.values()

    def _fetch_tables(
        self,
        cursor,
        database: str,
        account: str,
        filter: DatasetFilter,
    ) -> Dict[str, DatasetInfo]:
        try:
            cursor.execute("USE " + database)
        except snowflake.connector.errors.ProgrammingError:
            raise ValueError(f"Invalid or inaccessible database {database}")

        cursor.execute(SnowflakeExtractor.FETCH_TABLE_QUERY)

        tables: Dict[str, DatasetInfo] = {}
        for row in cursor:
            schema, name, table_type, row_count = row[0], row[1], row[2], row[4]
            if (
                not self.include_views
                and table_type != SnowflakeTableType.BASE_TABLE.value
            ):
                # exclude both view and temporary table
                continue

            full_name = dataset_fullname(database, schema, name)
            if not filter.include_table(database, schema, name):
                logger.info(f"Ignore {full_name} due to filter config")
                continue

            self._datasets[full_name] = self._init_dataset(account, full_name)
            tables[full_name] = DatasetInfo(
                database, schema, name, table_type, int(row_count)
            )

        return tables

    FETCH_COLUMNS_QUERY = """
        SELECT table_catalog, table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        ORDER BY ordinal_position
        """

    def _fetch_columns_async(
        self,
        conn: snowflake.connector.SnowflakeConnection,
        tables: Dict[str, DatasetInfo],
    ) -> None:

        # Create a map of full_name => [column_info] from information_schema.columns
        cursor = conn.cursor()
        cursor.execute(self.FETCH_COLUMNS_QUERY)
        column_info_map: Dict[str, List[Tuple[str, str]]] = {}
        for row in cursor:
            (
                table_catalog,
                table_schema,
                table_name,
                column_name,
                data_type,
            ) = row
            full_name = dataset_fullname(table_catalog, table_schema, table_name)
            if full_name not in tables:
                continue

            column_info_map.setdefault(full_name, []).append((column_name, data_type))

        # Build profile query for each table
        profile_queries = {}
        for full_name, column_info in column_info_map.items():
            dataset_info = tables.get(full_name)
            assert dataset_info is not None

            profile_queries[full_name] = QueryWithParam(
                SnowflakeProfileExtractor._build_profiling_query(
                    column_info,
                    dataset_info.schema,
                    dataset_info.name,
                    dataset_info.row_count,
                    self._column_statistics,
                    self._sampling,
                )
            )

        profiles = async_execute(
            conn, profile_queries, "profile_columns", self.max_concurrency
        )

        for full_name, profile in profiles.items():
            dataset = self._datasets[full_name]

            SnowflakeProfileExtractor._parse_profiling_result(
                column_info_map[full_name], profile[0], dataset, self._column_statistics
            )

    @staticmethod
    def _build_profiling_query(
        columns: List[Tuple[str, str]],
        schema: str,
        name: str,
        row_count: int,
        column_statistics: ColumnStatistics,
        sampling: SamplingConfig,
    ) -> str:
        query = ["SELECT COUNT(1)"]

        for column, data_type in columns:
            if (
                column_statistics.unique_count
                and not SnowflakeProfileExtractor._is_complex(data_type)
            ):
                query.append(f', COUNT(DISTINCT "{column}")')

            if column_statistics.null_count:
                query.append(f', COUNT(1) - COUNT("{column}")')

            if SnowflakeProfileExtractor._is_numeric(data_type):
                if column_statistics.min_value:
                    query.append(f', MIN("{column}")')
                if column_statistics.max_value:
                    query.append(f', MAX("{column}")')
                if column_statistics.avg_value:
                    query.append(f', AVG("{column}")')
                if column_statistics.std_dev:
                    query.append(f', STDDEV(CAST("{column}" as DOUBLE))')

        query.append(f' FROM "{schema}"."{name}"')

        if sampling.percentage < 100 and row_count >= sampling.threshold:
            query.append(f" SAMPLE SYSTEM ({sampling.percentage})")

        return "".join(query)

    @staticmethod
    def _parse_profiling_result(
        columns: List[Tuple[str, str]],
        results: Tuple,
        dataset: Dataset,
        column_statistics: ColumnStatistics,
    ) -> None:
        assert (
            dataset.field_statistics is not None
            and dataset.field_statistics.field_statistics is not None
        )
        fields = dataset.field_statistics.field_statistics

        assert len(results) > 0, f"Empty result for ${dataset.logical_id}"
        row_count = int(results[0])

        index = 1
        for column, data_type in columns:

            unique_count = None
            if (
                column_statistics.unique_count
                and not SnowflakeProfileExtractor._is_complex(data_type)
            ):
                unique_count = float(results[index])
                index += 1

            nulls, non_nulls = None, None
            if column_statistics.null_count:
                nulls = float(results[index])
                non_nulls = row_count - nulls
                index += 1

            min_value, max_value, avg, std_dev = None, None, None, None
            if SnowflakeProfileExtractor._is_numeric(data_type):
                if column_statistics.min_value:
                    min_value = (
                        None if results[index] is None else float(results[index])
                    )
                    index += 1

                if column_statistics.max_value:
                    max_value = (
                        None if results[index] is None else float(results[index])
                    )
                    index += 1

                if column_statistics.avg_value:
                    avg = None if results[index] is None else float(results[index])
                    index += 1

                if column_statistics.std_dev:
                    std_dev = None if results[index] is None else float(results[index])
                    index += 1

            fields.append(
                FieldStatistics(
                    field_path=column,
                    distinct_value_count=unique_count,
                    null_value_count=nulls,
                    nonnull_value_count=non_nulls,
                    min_value=min_value,
                    max_value=max_value,
                    average=avg,
                    std_dev=std_dev,
                )
            )

        # Verify that we've consumed all the elements from results
        assert index == len(
            results
        ), f"Unconsumed elements from results for {dataset.logical_id}"

    @staticmethod
    def _is_numeric(data_type: str) -> bool:
        return data_type in ("NUMBER", "FLOAT")

    @staticmethod
    def _is_complex(data_type: str) -> bool:
        # https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
        return data_type in ("VARIANT", "ARRAY", "OBJECT", "GEOGRAPHY")

    @staticmethod
    def _init_dataset(account: str, full_name: str) -> Dataset:
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=full_name, account=account, platform=DataPlatform.SNOWFLAKE
        )

        dataset.field_statistics = DatasetFieldStatistics(field_statistics=[])

        return dataset
