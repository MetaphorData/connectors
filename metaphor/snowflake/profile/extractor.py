import logging
from collections import defaultdict
from typing import Collection, Dict, List, NamedTuple, Tuple

try:
    from snowflake.connector import ProgrammingError, SnowflakeConnection
except ImportError:
    print("Please install metaphor[snowflake] extra\n")
    raise


from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    normalize_full_dataset_name,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.sampling import SamplingConfig
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.common.utils import safe_float, safe_int
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetFieldStatistics,
    DatasetLogicalID,
    FieldStatistics,
)
from metaphor.snowflake import auth
from metaphor.snowflake.extractor import DEFAULT_FILTER, SnowflakeExtractor
from metaphor.snowflake.profile.config import (
    ColumnStatistics,
    SnowflakeProfileRunConfig,
)
from metaphor.snowflake.utils import DatasetInfo, QueryWithParam, SnowflakeTableType

logger = get_logger()

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)


class Table(NamedTuple):
    catalog: str
    schema: str
    name: str

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"

    @classmethod
    def from_name(cls, name: str) -> "Table":
        catalog, schema, table_name = name.split(".")
        return Table(catalog, schema, table_name)


class SnowflakeProfileExtractor(BaseExtractor):
    """Snowflake data profile extractor"""

    _description = "Snowflake data profile crawler"
    _platform = Platform.SNOWFLAKE

    @staticmethod
    def from_config_file(config_file: str) -> "SnowflakeProfileExtractor":
        return SnowflakeProfileExtractor(
            SnowflakeProfileRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: SnowflakeProfileRunConfig):
        super().__init__(config)
        self._account = normalize_snowflake_account(config.account)
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)
        self._max_concurrency = config.max_concurrency
        self._include_views = config.include_views
        self._column_statistics = config.column_statistics
        self._sampling = config.sampling
        self._config = config

        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching data profile from Snowflake")

        self._conn = auth.connect(self._config)

        with self._conn:
            cursor = self._conn.cursor()

            databases = (
                SnowflakeExtractor.fetch_databases(cursor)
                if self._filter.includes is None
                else list(self._filter.includes.keys())
            )

            for database in databases:
                tables = self._fetch_tables(cursor, database)
                logger.info(
                    f"Include {len(tables)} {'tables/views' if self._include_views else 'tables'} from {database}"
                )

                self._fetch_columns_async(self._conn, tables)

        logger.debug(self._datasets)

        return self._datasets.values()

    def _fetch_tables(self, cursor, database: str) -> Dict[str, DatasetInfo]:
        try:
            cursor.execute("USE " + database)
        except ProgrammingError:
            raise ValueError(f"Invalid or inaccessible database {database}")

        cursor.execute(SnowflakeExtractor.FETCH_TABLE_QUERY)

        tables: Dict[str, DatasetInfo] = {}
        for row in cursor:
            schema, name, table_type, row_count = row[1], row[2], row[3], row[5]
            if (
                not self._include_views
                and table_type != SnowflakeTableType.BASE_TABLE.value
            ):
                # exclude both view and temporary table
                continue

            normalized_name = dataset_normalized_name(database, schema, name)
            if not self._filter.include_table(database, schema, name):
                logger.info(f"Ignore {normalized_name} due to filter config")
                continue

            self._datasets[normalized_name] = self._init_dataset(
                self._account, normalized_name
            )
            tables[normalized_name] = DatasetInfo(
                database, schema, name, table_type, safe_int(row_count)
            )

        return tables

    FETCH_COLUMNS_QUERY = """
        SELECT table_catalog, table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        ORDER BY ordinal_position
        """

    def _fetch_columns_async(
        self,
        connection: SnowflakeConnection,
        tables: Dict[str, DatasetInfo],
    ) -> None:
        # Create a map of full_name => [column_info] from information_schema.columns
        column_info_map: Dict[Table, List[Tuple[str, str]]] = defaultdict(list)
        profile_queries = {}

        cursor = connection.cursor()
        cursor.execute("SHOW COLUMNS")

        # This could include columns in views or stream tables.
        for row in cursor:
            # See https://docs.snowflake.com/en/sql-reference/sql/show-columns#output
            (
                table_name,
                table_schema,
                column_name,
                data_type,
                _,
                _,
                _,
                _,
                _,
                table_catalog,
                *_,
            ) = row
            column_info_map[Table(table_catalog, table_schema, table_name)].append(
                (column_name, data_type)
            )

        for table, column_info in column_info_map.items():
            row_count = None
            dataset_info = tables.get(
                normalize_full_dataset_name(table.full_name)
            )  # FIXME normalized name is case insensitive, make it case sensitive so that this logic works properly
            if dataset_info is None:
                continue

            row_count = dataset_info.row_count

            profile_queries[table.full_name] = QueryWithParam(
                SnowflakeProfileExtractor._build_profiling_query(
                    column_info,
                    table.schema,
                    table.name,
                    row_count or 0,
                    self._column_statistics,
                    self._sampling,
                )
            )

        # Has to be here, otherwise patching during test would not work
        from metaphor.snowflake.utils import async_execute

        profiles = async_execute(
            connection, profile_queries, "profile_columns", self._max_concurrency
        )

        for full_name, profile in profiles.items():
            # We need to make sure full_name points to an actual table,
            # instead of view or stream table.
            dataset = self._datasets.get(
                normalize_full_dataset_name(full_name)
            )  # FIXME normalized name is case insensitive, make it case sensitive so that this logic works properly

            if dataset:
                SnowflakeProfileExtractor._parse_profiling_result(
                    column_info_map[Table.from_name(full_name)],
                    profile[0],
                    dataset,
                    self._column_statistics,
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
    def _init_dataset(account: str, normalized_name: str) -> Dataset:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=normalized_name, account=account, platform=DataPlatform.SNOWFLAKE
        )

        dataset.field_statistics = DatasetFieldStatistics(field_statistics=[])

        return dataset
