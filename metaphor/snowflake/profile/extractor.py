import logging
from typing import Dict, List, Optional, Tuple

from snowflake.connector import SnowflakeConnection

from metaphor.common.event_util import EventUtil
from metaphor.common.filter import DatasetFilter, include_table
from metaphor.common.logger import get_logger
from metaphor.snowflake.auth import connect
from metaphor.snowflake.extractor import SnowflakeExtractor
from metaphor.snowflake.profile.config import SnowflakeProfileRunConfig
from metaphor.snowflake.utils import (
    DatasetInfo,
    QueryWithParam,
    SnowflakeTableType,
    async_execute,
)

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
    MetadataChangeEvent,
)

from metaphor.common.extractor import BaseExtractor

logger = get_logger(__name__)

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

SAMPLING_THRESHOLD = 100000  # the minimum number of rows in a table to do sampling. If row count smaller than this, sampling won't apply


class SnowflakeProfileExtractor(BaseExtractor):
    """Snowflake data profile extractor"""

    @staticmethod
    def config_class():
        return SnowflakeProfileRunConfig

    def __init__(self):
        self.max_concurrency = None
        self.include_views = None
        self._sampling_percentage = None
        self._datasets: Dict[str, Dataset] = {}

    async def extract(
        self, config: SnowflakeProfileRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeProfileExtractor.config_class())

        logger.info("Fetching data profile from Snowflake")
        self.max_concurrency = config.max_concurrency
        self.include_views = config.include_views

        assert config.sampling_percentage is None or (
            0 < config.sampling_percentage <= 100
        ), f"Invalid sample probability ${config.sampling_percentage}, value must be between 0 and 100"
        self._sampling_percentage = config.sampling_percentage

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

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

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

            full_name = SnowflakeExtractor.table_fullname(database, schema, name)
            if not include_table(database, schema, name, filter):
                logger.info(f"Ignore {full_name} due to filter config")
                continue

            self._datasets[full_name] = self._init_dataset(account, full_name)
            tables[full_name] = DatasetInfo(
                database, schema, name, table_type, int(row_count)
            )

        return tables

    FETCH_COLUMNS_QUERY = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """

    def _fetch_columns_async(
        self, conn: SnowflakeConnection, tables: Dict[str, DatasetInfo]
    ) -> None:
        queries = {
            fullname: QueryWithParam(
                self.FETCH_COLUMNS_QUERY, (dataset.schema, dataset.name)
            )
            for fullname, dataset in tables.items()
        }
        results = async_execute(conn, queries, "fetch_columns", self.max_concurrency)

        column_info_map = {}
        profile_queries = {}
        for full_name, columns in results.items():
            dataset = tables[full_name]
            column_info = [
                (name, data_type, nullable == "YES")
                for name, data_type, nullable in columns
            ]
            column_info_map[full_name] = column_info
            profile_queries[full_name] = QueryWithParam(
                SnowflakeProfileExtractor._build_profiling_query(
                    column_info,
                    dataset.schema,
                    dataset.name,
                    dataset.row_count,
                    self._sampling_percentage,
                )
            )

        profiles = async_execute(
            conn, profile_queries, "profile_columns", self.max_concurrency
        )

        for full_name, profile in profiles.items():
            dataset = self._datasets[full_name]

            SnowflakeProfileExtractor._parse_profiling_result(
                column_info_map[full_name], profile[0], dataset
            )

    @staticmethod
    def _build_profiling_query(
        columns: List[Tuple[str, str, bool]],
        schema: str,
        name: str,
        row_count: int,
        sampling_percentage: Optional[float],
    ) -> str:
        query = ["SELECT COUNT(1) ROW_COUNT"]

        for column, data_type, nullable in columns:
            query.append(f', COUNT(DISTINCT "{column}")')

            if nullable:
                query.append(f', COUNT_IF("{column}" is NULL)')

            if SnowflakeProfileExtractor._is_numeric(data_type):
                query.extend(
                    [
                        f', MIN("{column}")',
                        f', MAX("{column}")',
                        f', AVG("{column}")',
                    ]
                )

        query.append(f' FROM "{schema}"."{name}"')

        if row_count >= SAMPLING_THRESHOLD and sampling_percentage is not None:
            query.append(f" SAMPLE SYSTEM ({sampling_percentage})")

        return "".join(query)

    @staticmethod
    def _parse_profiling_result(
        columns: List[Tuple[str, str, bool]], results: Tuple, dataset: Dataset
    ) -> None:
        assert (
            dataset.field_statistics is not None
            and dataset.field_statistics.field_statistics is not None
        )
        fields = dataset.field_statistics.field_statistics

        assert len(results) > 1
        row_count = int(results[0])

        index = 1
        for column, data_type, nullable in columns:
            unique_values = float(results[index])
            index += 1

            if nullable:
                nulls = float(results[index]) if results[index] else 0.0
                index += 1
            else:
                nulls = 0.0

            if SnowflakeProfileExtractor._is_numeric(data_type):
                min_value = float(results[index]) if results[index] else None
                index += 1
                max_value = float(results[index]) if results[index] else None
                index += 1
                avg = float(results[index]) if results[index] else None
                index += 1
            else:
                min_value, max_value, avg = None, None, None

            fields.append(
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
    def _is_numeric(data_type: str) -> bool:
        return data_type in ["NUMBER", "FLOAT"]

    @staticmethod
    def _init_dataset(account: str, full_name: str) -> Dataset:
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=full_name, account=account, platform=DataPlatform.SNOWFLAKE
        )

        dataset.field_statistics = DatasetFieldStatistics(field_statistics=[])

        return dataset
