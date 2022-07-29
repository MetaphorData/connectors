from datetime import datetime, timezone
from typing import Collection, Dict, List, Mapping, Optional

from metaphor.models.crawler_run_metadata import Platform
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

from metaphor.common.entity_id import dataset_fullname
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.snowflake.auth import connect
from metaphor.snowflake.config import SnowflakeRunConfig
from metaphor.snowflake.utils import DatasetInfo, SnowflakeTableType

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
    SchemaField,
    SchemaType,
    SourceInfo,
    SQLSchema,
)

from metaphor.common.extractor import BaseExtractor

logger = get_logger(__name__)


class SnowflakeExtractor(BaseExtractor):
    """Snowflake metadata extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.SNOWFLAKE

    def description(self) -> str:
        return "Snowflake metadata crawler"

    @staticmethod
    def config_class():
        return SnowflakeRunConfig

    def __init__(self):
        self.account = None
        self.max_concurrency = None
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self, config: SnowflakeRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, SnowflakeExtractor.config_class())

        logger.info("Fetching metadata from Snowflake")
        self.account = config.account
        self.max_concurrency = config.max_concurrency

        conn = connect(config)

        with conn:
            cursor = conn.cursor()

            filter = config.filter.normalize()

            databases = (
                self.fetch_databases(cursor)
                if filter.includes is None
                else list(filter.includes.keys())
            )

            logger.info(f"Databases to include: {databases}")

            for database in databases:
                tables = self._fetch_tables(cursor, database, filter)
                if len(tables) == 0:
                    logger.info(f"Skip empty database {database}")
                    continue

                logger.info(f"Include {len(tables)} tables from {database}")

                self._fetch_columns(cursor, database)
                self._fetch_primary_keys(cursor, database)
                self._fetch_unique_keys(cursor, database)
                self._fetch_table_info(conn, tables)

            self._fetch_tags(cursor)

        logger.debug(self._datasets)

        return self._datasets.values()

    @staticmethod
    def fetch_databases(cursor: SnowflakeCursor) -> List[str]:
        cursor.execute(
            "SELECT database_name FROM information_schema.databases ORDER BY database_name"
        )
        return [db[0] for db in cursor]

    FETCH_TABLE_QUERY = """
    SELECT table_schema, table_name, table_type, COMMENT, row_count, bytes
    FROM information_schema.tables
    WHERE table_schema != 'INFORMATION_SCHEMA'
    ORDER BY table_schema, table_name
    """

    def _fetch_tables(
        self, cursor: SnowflakeCursor, database: str, filter: DatasetFilter
    ) -> Dict[str, DatasetInfo]:
        try:
            cursor.execute("USE " + database)
        except snowflake.connector.errors.ProgrammingError:
            raise ValueError(f"Invalid or inaccessible database {database}")

        cursor.execute(self.FETCH_TABLE_QUERY)

        tables: Dict[str, DatasetInfo] = {}
        for schema, name, table_type, comment, row_count, table_bytes in cursor:
            full_name = dataset_fullname(database, schema, name)
            if not filter.include_table(database, schema, name):
                logger.info(f"Ignore {full_name} due to filter config")
                continue

            # TODO: Support dots in database/schema/table name
            if "." in database or "." in schema or "." in name:
                logger.info(
                    f"Ignore {full_name} due to dot in database, schema, or table name"
                )
                continue

            self._datasets[full_name] = self._init_dataset(
                full_name, table_type, comment, row_count, table_bytes
            )
            tables[full_name] = DatasetInfo(database, schema, name, table_type)

        return tables

    def _fetch_columns(self, cursor: SnowflakeCursor, database: str) -> None:
        cursor.execute(
            """
            SELECT table_schema, table_name, column_name, data_type, character_maximum_length,
              numeric_precision, is_nullable, column_default, comment
            FROM information_schema.columns
            WHERE table_schema != 'INFORMATION_SCHEMA'
            ORDER BY table_schema, table_name, ordinal_position
            """
        )

        for (
            table_schema,
            table_name,
            column,
            data_type,
            max_length,
            precision,
            nullable,
            default,
            comment,
        ) in cursor:
            full_name = dataset_fullname(database, table_schema, table_name)
            if full_name not in self._datasets:
                continue

            dataset = self._datasets[full_name]

            assert dataset.schema is not None and dataset.schema.fields is not None

            dataset.schema.fields.append(
                SchemaField(
                    field_path=column,
                    native_type=data_type,
                    max_length=float(max_length) if max_length is not None else None,
                    precision=float(precision) if precision is not None else None,
                    nullable=nullable == "YES",
                    description=comment,
                )
            )

    def _fetch_table_info(
        self, conn: SnowflakeConnection, tables: Dict[str, DatasetInfo]
    ) -> None:
        queries, params = [], []
        ddl_tables, updated_time_tables = [], []
        for fullname, table in tables.items():
            # fetch last_update_time and DDL for tables, and fetch only DDL for views
            if table.type == SnowflakeTableType.BASE_TABLE.value:
                queries.append(
                    f'SYSTEM$LAST_CHANGE_COMMIT_TIME(%s) as "UPDATED_{fullname}"'
                )
                params.append(fullname)
                updated_time_tables.append(fullname)

            queries.append(f"get_ddl('table', %s) as \"DDL_{fullname}\"")
            params.append(fullname)
            ddl_tables.append(fullname)

        query = f"SELECT {','.join(queries)}"

        cursor = conn.cursor(DictCursor)

        try:
            cursor.execute(query, tuple(params))
        except Exception as e:
            logger.exception(f"Failed to execute query:\n{query}\n{e}")
            return

        results = cursor.fetchone()
        assert isinstance(results, Mapping)
        cursor.close()

        for fullname in ddl_tables:
            dataset = self._datasets[fullname]
            assert dataset.schema is not None and dataset.schema.sql_schema is not None

            dataset.schema.sql_schema.table_schema = results[f"DDL_{fullname}"]

        for fullname in updated_time_tables:
            dataset = self._datasets[fullname]
            assert dataset.schema.sql_schema is not None

            timestamp = results[f"UPDATED_{fullname}"]
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
            table = dataset_fullname(database, schema, table_name)

            dataset = self._datasets.get(table)
            if dataset is None or dataset.schema is None:
                logger.warning(
                    f"Table {table} schema not found for unique key {constraint_name}"
                )
                continue

            field = next(
                (f for f in dataset.schema.fields if f.field_path == column),
                None,
            )
            if not field:
                logger.warning(
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
            table = dataset_fullname(database, schema, table_name)

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

    def _fetch_tags(self, cursor: SnowflakeCursor) -> None:
        cursor.execute(
            """
            SELECT TAG_NAME, TAG_VALUE, OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME
            FROM snowflake.account_usage.tag_references
            WHERE domain = 'TABLE'
            ORDER BY OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME
            """
        )

        for key, value, database, schema, table_name in cursor:
            table = dataset_fullname(database, schema, table_name)

            dataset = self._datasets.get(table)
            if dataset is None or dataset.schema is None:
                logger.error(
                    f"Table {table} not found for tag {self._build_tag_string(key, value)}"
                )
                continue

            if not dataset.schema.tags:
                dataset.schema.tags = []
            dataset.schema.tags.append(self._build_tag_string(key, value))

    @staticmethod
    def _build_tag_string(tag_key: str, tag_value: str) -> str:
        return f"{tag_key}={tag_value}"

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
            if table_type == SnowflakeTableType.VIEW.value
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
