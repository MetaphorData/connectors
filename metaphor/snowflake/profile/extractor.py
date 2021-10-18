import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from metaphor.common.event_util import EventUtil
from metaphor.snowflake.extractor import SnowflakeExtractor

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
from serde import deserialize

from metaphor.common.extractor import BaseExtractor, RunConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class SnowflakeProfileRunConfig(RunConfig):
    account: str
    user: str
    password: str
    default_database: str

    # A list of databases to include. Includes all databases if not specified.
    target_databases: Optional[List[str]]


class SnowflakeProfileExtractor(BaseExtractor):
    """Snowflake data profile extractor"""

    @staticmethod
    def config_class():
        return SnowflakeProfileRunConfig

    def __init__(self):
        self._datasets: Dict[str, Dataset] = {}

    async def extract(
        self, config: SnowflakeProfileRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeProfileExtractor.config_class())

        logger.info(f"Fetching data profile from Snowflake account {config.account}")
        ctx = snowflake.connector.connect(
            account=config.account, user=config.user, password=config.password
        )

        with ctx:
            cursor = ctx.cursor()

            databases = (
                SnowflakeExtractor.fetch_databases(cursor, config.default_database)
                if config.target_databases is None
                else config.target_databases
            )
            logger.info(f"Databases to include: {databases}")

            for database in databases:
                tables = self._fetch_tables(cursor, database, config.account)
                logger.info(f"DB {database} has tables: {tables}")

                for schema, name, full_name in tables:
                    logger.info(f"Profiling table {full_name}")
                    dataset = self._datasets[full_name]
                    try:
                        self._fetch_columns(cursor, schema, name, dataset)
                    except Exception as e:
                        logger.error(f"Failed to profile {full_name}:\n{e}")

        logger.debug(self._datasets)

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _fetch_tables(
        self, cursor, database: str, account: str
    ) -> List[Tuple[str, str, str]]:
        try:
            cursor.execute("USE " + database)
        except snowflake.connector.errors.ProgrammingError:
            raise ValueError(f"Invalid or inaccessible database {database}")

        cursor.execute(SnowflakeExtractor.FETCH_TABLE_QUERY)

        tables: List[Tuple[str, str, str]] = []
        for row in cursor:
            schema, name = row[0], row[1]
            full_name = SnowflakeExtractor.table_fullname(database, schema, name)
            self._datasets[full_name] = self._init_dataset(account, full_name)
            tables.append((schema, name, full_name))

        return tables

    @staticmethod
    def _fetch_columns(cursor, schema: str, name: str, dataset: Dataset) -> None:

        cursor.execute(SnowflakeExtractor.FETCH_COLUMNS_QUERY, (schema, name))

        # (column, data type, nullable)
        columns = [(row[1], row[2], row[5] == "YES") for row in cursor]

        cursor.execute(
            SnowflakeProfileExtractor._build_profiling_query(columns, schema, name)
        )

        SnowflakeProfileExtractor._parse_profiling_result(
            columns, cursor.fetchone(), dataset
        )

    @staticmethod
    def _build_profiling_query(
        columns: List[Tuple[str, str, bool]], schema: str, name: str
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
                        f', STDDEV("{column}")',
                    ]
                )

        query.append(f' FROM "{schema}"."{name}"')

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
                stddev = float(results[index]) if results[index] else None
                index += 1
            else:
                min_value, max_value, avg, stddev = None, None, None, None

            fields.append(
                FieldStatistics(
                    field_path=column,
                    distinct_value_count=unique_values,
                    null_value_count=nulls,
                    nonnull_value_count=(row_count - nulls),
                    min_value=min_value,
                    max_value=max_value,
                    average=avg,
                    std_dev=stddev,
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
