import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from metaphor.common.event_util import EventUtil
from metaphor.snowflake.auth import SnowflakeAuthConfig, connect
from metaphor.snowflake.utils import (
    DEFAULT_THREAD_POOL_SIZE,
    DatasetInfo,
    QueryWithParam,
    async_execute,
)

try:
    import snowflake.connector
except ImportError:
    print("Please install metaphor[snowflake] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    AspectType,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    EntityType,
    MaterializationType,
    MetadataChangeEvent,
    SchemaField,
    SchemaType,
    SourceInfo,
    SQLSchema,
)
from serde import deserialize

from metaphor.common.extractor import BaseExtractor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class SnowflakeRunConfig(SnowflakeAuthConfig):
    # A list of databases to include. Includes all databases if not specified.
    target_databases: Optional[List[str]] = None

    # max number of concurrent queries to database
    max_concurrency: Optional[int] = DEFAULT_THREAD_POOL_SIZE


class SnowflakeExtractor(BaseExtractor):
    """Snowflake metadata extractor"""

    @staticmethod
    def config_class():
        return SnowflakeRunConfig

    def __init__(self):
        self.account = None
        self.max_concurrency = None
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self, config: SnowflakeRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeExtractor.config_class())

        logger.info("Fetching metadata from Snowflake")
        self.account = config.account
        self.max_concurrency = config.max_concurrency

        conn = connect(config)

        with conn:
            cursor = conn.cursor()

            databases = (
                self.fetch_databases(cursor)
                if config.target_databases is None
                else config.target_databases
            )
            logger.info(f"Databases to include: {databases}")

            for database in databases:
                tables = self._fetch_tables(cursor, database)
                logger.info(f"DB {database} has tables: {tables}")

                self._fetch_columns_async(conn, tables)
                self._fetch_table_info_async(conn, tables)

                self._fetch_primary_keys(cursor, database)
                self._fetch_unique_keys(cursor, database)

        logger.debug(self._datasets)

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    @staticmethod
    def fetch_databases(cursor: SnowflakeCursor) -> List[str]:
        cursor.execute(
            "SELECT database_name FROM information_schema.databases ORDER BY database_name"
        )
        return [db[0] for db in cursor]

    @staticmethod
    def table_fullname(db: str, schema: str, name: str):
        """The full table name including database, schema and name"""
        return f"{db}.{schema}.{name}".lower()

    FETCH_TABLE_QUERY = """
    SELECT table_schema, table_name, table_type, COMMENT, row_count, bytes
    FROM information_schema.tables
    WHERE table_schema != 'INFORMATION_SCHEMA'
    ORDER BY table_schema, table_name
    """

    def _fetch_tables(
        self, cursor: SnowflakeCursor, database: str
    ) -> Dict[str, DatasetInfo]:
        try:
            cursor.execute("USE " + database)
        except snowflake.connector.errors.ProgrammingError:
            raise ValueError(f"Invalid or inaccessible database {database}")

        cursor.execute(self.FETCH_TABLE_QUERY)

        tables: Dict[str, DatasetInfo] = {}
        for schema, name, table_type, comment, row_count, table_bytes in cursor:
            full_name = self.table_fullname(database, schema, name)
            self._datasets[full_name] = self._init_dataset(
                full_name, table_type, comment, row_count, table_bytes
            )
            tables[full_name] = DatasetInfo(database, schema, name, table_type)

        return tables

    FETCH_COLUMNS_QUERY = """
    SELECT ordinal_position, column_name, data_type, character_maximum_length,
      numeric_precision, is_nullable, column_default, comment
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """

    def _fetch_columns_async(
        self, conn: SnowflakeConnection, tables: Dict[str, DatasetInfo]
    ) -> None:
        queries = {
            fullname: QueryWithParam(
                SnowflakeExtractor.FETCH_COLUMNS_QUERY, (dataset.schema, dataset.name)
            )
            for fullname, dataset in tables.items()
        }
        results = async_execute(
            conn,
            queries,
            "fetch_columns",
            self.max_concurrency,
        )

        for full_name, columns in results.items():
            dataset = self._datasets[full_name]
            assert dataset.schema is not None and dataset.schema.fields is not None

            for column in columns:
                dataset.schema.fields.append(SnowflakeExtractor._build_field(column))

    FETCH_TABLE_INFO_QUERY = """
    SELECT get_ddl('table', %s) as ddl, SYSTEM$LAST_CHANGE_COMMIT_TIME(%s) as last_change_time
    """

    FETCH_DDL_QUERY = "SELECT get_ddl('table', %s) as ddl"

    def _fetch_table_info_async(
        self, conn: SnowflakeConnection, tables: Dict[str, DatasetInfo]
    ) -> None:
        # fetch last_update_time and DDL for tables, and fetch only DDL for views
        queries = {
            fullname: QueryWithParam(
                self.FETCH_TABLE_INFO_QUERY, (f"{dataset.schema}.{dataset.name}",) * 2
            )
            if dataset.type == "BASE TABLE"
            else QueryWithParam(
                self.FETCH_DDL_QUERY, (f"{dataset.schema}.{dataset.name}",)
            )
            for fullname, dataset in tables.items()
        }
        results = async_execute(
            conn,
            queries,
            "fetch_table_info",
            self.max_concurrency,
        )

        for full_name, result in results.items():
            table_info = result[0]
            dataset = self._datasets[full_name]
            assert (
                dataset.schema is not None
                and dataset.schema.sql_schema is not None
                and dataset.statistics is not None
            )

            dataset.schema.sql_schema.table_schema = table_info[0]

            if len(table_info) == 1:
                continue

            timestamp = table_info[1]
            if timestamp > 0:
                dataset.statistics.last_updated = datetime.utcfromtimestamp(
                    timestamp / 1000
                ).replace(tzinfo=timezone.utc)

    def _fetch_unique_keys(self, cursor: SnowflakeCursor, database: str) -> None:
        cursor.execute(f"SHOW UNIQUE KEYS IN DATABASE {database}")

        for entry in cursor:
            schema, table_name, column, constraint_name = (
                entry[2],
                entry[3],
                entry[4],
                entry[6],
            )
            table = self.table_fullname(database, schema, table_name)

            dataset = self._datasets.get(table)
            if dataset is None or dataset.schema is None:
                logger.error(
                    f"Table {table} schema not found for unique key {constraint_name}"
                )
                continue

            field = next(
                (f for f in dataset.schema.fields if f.field_path == column),
                None,
            )
            if not field:
                logger.error(
                    f"Column {column} not found in table {table} for unique key {constraint_name}"
                )
                continue

            field.is_unique = True

    def _fetch_primary_keys(self, cursor: SnowflakeCursor, database: str) -> None:
        cursor.execute(f"SHOW PRIMARY KEYS IN DATABASE {database}")

        for entry in cursor:
            schema, table_name, column, constraint_name = (
                entry[2],
                entry[3],
                entry[4],
                entry[6],
            )
            table = self.table_fullname(database, schema, table_name)

            dataset = self._datasets.get(table)
            if dataset is None or dataset.schema is None:
                logger.error(
                    f"Table {table} schema not found for primary key {constraint_name}"
                )
                continue

            sql_schema = dataset.schema.sql_schema
            assert sql_schema is not None

            if sql_schema.primary_key is None:
                sql_schema.primary_key = []
            sql_schema.primary_key.append(column)

    def _init_dataset(
        self,
        full_name: str,
        table_type: str,
        comment: str,
        row_count: Optional[int],
        table_bytes: Optional[float],
    ) -> Dataset:
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=full_name, account=self.account, platform=DataPlatform.SNOWFLAKE
        )

        dataset.source_info = SourceInfo(
            main_url=SnowflakeExtractor.build_table_url(self.account, full_name)
        )

        dataset.schema = DatasetSchema()
        dataset.schema.aspect_type = AspectType.DATASET_SCHEMA
        dataset.schema.schema_type = SchemaType.SQL
        dataset.schema.description = comment
        dataset.schema.fields = []
        dataset.schema.sql_schema = SQLSchema()
        dataset.schema.sql_schema.materialization = (
            MaterializationType.VIEW
            if table_type == "VIEW"
            else MaterializationType.TABLE
        )

        dataset.statistics = DatasetStatistics()
        if row_count:
            dataset.statistics.record_count = float(row_count)
        if table_bytes:
            dataset.statistics.data_size = table_bytes / (1000 * 1000)  # in MB

        return dataset

    @staticmethod
    def build_table_url(account: str, full_name: str) -> str:
        db, schema, table = full_name.upper().split(".")
        return (
            f"https://{account}.snowflakecomputing.com/console#/data/tables/detail?"
            f"databaseName={db}&schemaName={schema}&tableName={table}"
        )

    @staticmethod
    def _build_field(column) -> SchemaField:
        return SchemaField(
            field_path=column[1],
            native_type=column[2],
            max_length=float(column[3]) if column[3] is not None else None,
            precision=float(column[4]) if column[4] is not None else None,
            nullable=column[5] == "YES",
            description=column[7],
        )
