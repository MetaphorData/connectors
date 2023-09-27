import math
from datetime import datetime, timezone
from typing import Collection, Dict, List, Mapping, Optional, Tuple

from pydantic import parse_raw_as

try:
    from snowflake.connector.cursor import DictCursor, SnowflakeCursor
    from snowflake.connector.errors import ProgrammingError
except ImportError:
    print("Please install metaphor[snowflake] extra\n")
    raise

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.query_history import chunk_query_logs
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.common.tag_matcher import tag_datasets
from metaphor.common.utils import chunks, md5_digest, safe_float, start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    EntityType,
    MaterializationType,
    QueriedDataset,
    QueryLog,
    SchemaField,
    SchemaType,
    SourceInfo,
    SQLSchema,
)
from metaphor.snowflake import auth
from metaphor.snowflake.accessed_object import AccessedObject
from metaphor.snowflake.config import SnowflakeRunConfig
from metaphor.snowflake.utils import (
    DatasetInfo,
    QueryWithParam,
    SnowflakeTableType,
    async_execute,
    check_access_history,
    exclude_username_clause,
    fetch_query_history_count,
    to_quoted_identifier,
)

logger = get_logger()

# Filter out "Snowflake" database & all "information_schema" schemas
DEFAULT_FILTER: DatasetFilter = DatasetFilter(
    excludes={
        "SNOWFLAKE": None,
        "*": {"INFORMATION_SCHEMA": None},
    }
)

# max number of tables' information to fetch in one query
TABLE_INFO_FETCH_SIZE = 100


class SnowflakeExtractor(BaseExtractor):
    """Snowflake metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SnowflakeExtractor":
        return SnowflakeExtractor(SnowflakeRunConfig.from_yaml_file(config_file))

    def __init__(self, config: SnowflakeRunConfig):
        super().__init__(config, "Snowflake metadata crawler", Platform.SNOWFLAKE)
        self._account = normalize_snowflake_account(config.account)
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)
        self._tag_matchers = config.tag_matchers
        self._query_log_excluded_usernames = config.query_log.excluded_usernames
        self._query_log_lookback_days = config.query_log.lookback_days
        self._query_log_fetch_size = config.query_log.fetch_size
        self._max_concurrency = config.max_concurrency
        self._config = config

        self._datasets: Dict[str, Dataset] = {}
        self._logs: List[QueryLog] = []

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Snowflake")

        self._conn = auth.connect(self._config)

        with self._conn:
            cursor = self._conn.cursor()

            databases = (
                self.fetch_databases(cursor)
                if self._filter.includes is None
                else list(self._filter.includes.keys())
            )
            logger.info(f"Databases to include: {databases}")

            shared_databases = self._fetch_shared_databases(cursor)
            logger.info(f"Shared inbound databases: {shared_databases}")

            for database in databases:
                tables = self._fetch_tables(cursor, database)
                if len(tables) == 0:
                    logger.info(f"Skip empty database {database}")
                    continue

                logger.info(f"Include {len(tables)} tables from {database}")

                self._fetch_columns(cursor, database)

                try:
                    self._fetch_table_info(tables, database in shared_databases)
                except Exception as e:
                    logger.exception(
                        f"Failed to fetch table extra info for '{database}'\n{e}"
                    )

            self._fetch_primary_keys(cursor)
            self._fetch_unique_keys(cursor)
            self._fetch_tags(cursor)

            if self._query_log_lookback_days > 0:
                self._fetch_query_logs()

        datasets = list(self._datasets.values())
        tag_datasets(datasets, self._tag_matchers)

        entities: List[ENTITY_TYPES] = []
        entities.extend(datasets)
        entities.extend(chunk_query_logs(self._logs))
        return entities

    @staticmethod
    def fetch_databases(cursor: SnowflakeCursor) -> List[str]:
        cursor.execute(
            "SELECT database_name FROM information_schema.databases ORDER BY database_name"
        )
        return [db[0].lower() for db in cursor]

    @staticmethod
    def _fetch_shared_databases(cursor: SnowflakeCursor) -> List[str]:
        cursor.execute("SHOW SHARES")
        return [db[3].lower() for db in cursor if db[1] == "INBOUND"]

    FETCH_TABLE_QUERY = """
    SELECT table_catalog, table_schema, table_name, table_type, COMMENT, row_count, bytes
    FROM information_schema.tables
    WHERE table_schema != 'INFORMATION_SCHEMA'
    ORDER BY table_schema, table_name
    """

    def _fetch_tables(
        self, cursor: SnowflakeCursor, database_name: str
    ) -> Dict[str, DatasetInfo]:
        try:
            cursor.execute("USE " + database_name)
        except ProgrammingError:
            raise ValueError(f"Invalid or inaccessible database {database_name}")

        cursor.execute(self.FETCH_TABLE_QUERY)

        tables: Dict[str, DatasetInfo] = {}
        for (
            database,
            schema,
            name,
            table_type,
            comment,
            row_count,
            table_bytes,
        ) in cursor:
            normalized_name = dataset_normalized_name(database, schema, name)
            if not self._filter.include_table(database, schema, name):
                logger.info(f"Ignore {normalized_name} due to filter config")
                continue

            # TODO: Support dots in database/schema/table name
            if "." in database or "." in schema or "." in name:
                logger.warning(
                    f"Ignore {normalized_name} due to dot in database, schema, or table name"
                )
                continue

            self._datasets[normalized_name] = self._init_dataset(
                database, schema, name, table_type, comment, row_count, table_bytes
            )
            tables[normalized_name] = DatasetInfo(database, schema, name, table_type)

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
            normalized_name = dataset_normalized_name(
                database, table_schema, table_name
            )
            if normalized_name not in self._datasets:
                continue

            dataset = self._datasets[normalized_name]

            assert dataset.schema is not None and dataset.schema.fields is not None

            dataset.schema.fields.append(
                SchemaField(
                    field_path=column,
                    field_name=column,
                    native_type=data_type,
                    max_length=safe_float(max_length),
                    precision=safe_float(precision),
                    nullable=nullable == "YES",
                    description=comment,
                    subfields=None,
                )
            )

    def _fetch_table_info(
        self, tables: Dict[str, DatasetInfo], is_shared_database: bool
    ) -> None:
        dict_cursor: DictCursor = self._conn.cursor(DictCursor)  # type: ignore
        for chunk in chunks(list(tables.items()), TABLE_INFO_FETCH_SIZE):
            self._fetch_table_info_internal(dict_cursor, chunk, is_shared_database)

        dict_cursor.close()

    def _fetch_table_info_internal(
        self,
        dict_cursor: DictCursor,
        tables: List[Tuple[str, DatasetInfo]],
        is_shared_database: bool,
    ) -> None:
        queries, params = [], []
        ddl_tables, updated_time_tables = [], []
        for normalized_name, table in tables:
            fullname = to_quoted_identifier([table.database, table.schema, table.name])
            # fetch last_update_time and DDL for tables, and fetch only DDL for views
            if table.type == SnowflakeTableType.BASE_TABLE.value:
                queries.append(
                    f'SYSTEM$LAST_CHANGE_COMMIT_TIME(%s) as "UPDATED_{normalized_name}"'
                )
                params.append(fullname)
                updated_time_tables.append(normalized_name)

            # shared database doesn't support getting DDL
            if not is_shared_database:
                queries.append(f"get_ddl('table', %s) as \"DDL_{normalized_name}\"")
                params.append(fullname)
                ddl_tables.append(normalized_name)

        if not queries:
            return
        query = f"SELECT {','.join(queries)}"
        logger.debug(query)

        dict_cursor.execute(query, tuple(params))
        results = dict_cursor.fetchone()
        assert isinstance(results, Mapping)

        for normalized_name in ddl_tables:
            dataset = self._datasets[normalized_name]
            assert dataset.schema is not None and dataset.schema.sql_schema is not None

            dataset.schema.sql_schema.table_schema = results[f"DDL_{normalized_name}"]

        for normalized_name in updated_time_tables:
            dataset = self._datasets[normalized_name]
            assert dataset.schema.sql_schema is not None

            # Timestamp is in nanosecond.
            # See https://docs.snowflake.com/en/sql-reference/functions/system_last_change_commit_time.html
            timestamp = results[f"UPDATED_{normalized_name}"]
            if timestamp > 0:
                dataset.statistics.last_updated = datetime.utcfromtimestamp(
                    timestamp / 1000000000
                ).replace(tzinfo=timezone.utc)

    def _fetch_unique_keys(self, cursor: SnowflakeCursor) -> None:
        cursor.execute("SHOW UNIQUE KEYS")

        for entry in cursor:
            database, schema, table_name, column, constraint_name = (
                entry[1],
                entry[2],
                entry[3],
                entry[4],
                entry[6],
            )
            normalized_name = dataset_normalized_name(database, schema, table_name)

            dataset = self._datasets.get(normalized_name)
            if dataset is None or dataset.schema is None:
                logger.warning(
                    f"Table {normalized_name} schema not found for unique key {constraint_name}"
                )
                continue

            field = next(
                (f for f in dataset.schema.fields if f.field_path == column),
                None,
            )
            if not field:
                logger.warning(
                    f"Column {column} not found in table {normalized_name} for unique key {constraint_name}"
                )
                continue

            field.is_unique = True

    def _fetch_primary_keys(self, cursor: SnowflakeCursor) -> None:
        cursor.execute("SHOW PRIMARY KEYS")

        for entry in cursor:
            database, schema, table_name, column, constraint_name = (
                entry[1],
                entry[2],
                entry[3],
                entry[4],
                entry[6],
            )
            normalized_name = dataset_normalized_name(database, schema, table_name)

            dataset = self._datasets.get(normalized_name)
            if dataset is None or dataset.schema is None:
                logger.error(
                    f"Table {normalized_name} schema not found for primary key {constraint_name}"
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
            SELECT TAG_NAME, TAG_VALUE, DOMAIN, OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME
            FROM snowflake.account_usage.tag_references
            WHERE DOMAIN in ('TABLE', 'COLUMN')
            ORDER BY OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME
            """
        )

        for key, value, object_type, database, schema, table_name, column in cursor:
            normalized_dataset_name = dataset_normalized_name(
                database, schema, table_name
            )
            tag = self._build_tag_string(key, value)

            dataset = self._datasets.get(normalized_dataset_name)
            if dataset is None or dataset.schema is None:
                logger.error(f"Table {normalized_dataset_name} not found for tag {tag}")
                continue

            if object_type == "TABLE":
                if not dataset.schema.tags:
                    dataset.schema.tags = []
                dataset.schema.tags.append(tag)
            elif object_type == "COLUMN" and column:
                field = next(
                    fd
                    for fd in dataset.schema.fields
                    if fd.field_path.upper() == column.upper()
                )
                if not field:
                    logger.error(
                        f"Field {column} not found in {normalized_dataset_name} for tag {tag}"
                    )
                    continue
                if not field.tags:
                    field.tags = []
                field.tags.append(tag)

    def _fetch_query_logs(self) -> None:
        logger.info("Fetching Snowflake query logs")

        start_date = start_of_day(self._query_log_lookback_days)
        end_date = start_of_day()

        has_access_history = check_access_history(self._conn)
        logger.info(f"Using Snowflake Enterprise edition: {has_access_history}")

        count = fetch_query_history_count(
            self._conn,
            start_date,
            self._query_log_excluded_usernames,
            end_date,
            has_access_history,
        )
        batches = math.ceil(count / self._query_log_fetch_size)
        logger.info(f"Total {count} queries, dividing into {batches} batches")

        queries = (
            self._batch_query_for_access_logs(start_date, end_date, batches)
            if has_access_history
            else self._batch_query_for_query_logs(start_date, end_date, batches)
        )

        async_execute(
            self._conn,
            queries,
            "fetch_query_logs",
            self._max_concurrency,
            self._parse_query_logs,
        )

        logger.info(f"Fetched {len(self._logs)} query logs")

    def _batch_query_for_access_logs(
        self, start_date: datetime, end_date: datetime, batches: int
    ):
        return {
            x: QueryWithParam(
                f"""
                SELECT q.QUERY_ID, q.USER_NAME, QUERY_TEXT, START_TIME, TOTAL_ELAPSED_TIME, CREDITS_USED_CLOUD_SERVICES,
                  DATABASE_NAME, SCHEMA_NAME, BYTES_SCANNED, BYTES_WRITTEN, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED,
                  DIRECT_OBJECTS_ACCESSED, OBJECTS_MODIFIED
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                  ON a.QUERY_ID = q.QUERY_ID
                WHERE EXECUTION_STATUS = 'SUCCESS'
                  AND START_TIME > %s AND START_TIME <= %s
                  AND QUERY_START_TIME > %s AND QUERY_START_TIME <= %s
                  {exclude_username_clause(self._query_log_excluded_usernames)}
                ORDER BY q.QUERY_ID
                LIMIT {self._query_log_fetch_size} OFFSET %s
                """,
                (
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    *self._query_log_excluded_usernames,
                    x * self._query_log_fetch_size,
                ),
            )
            for x in range(batches)
        }

    def _batch_query_for_query_logs(
        self, start_date: datetime, end_date: datetime, batches: int
    ):
        return {
            x: QueryWithParam(
                f"""
                SELECT QUERY_ID, USER_NAME, QUERY_TEXT, START_TIME, TOTAL_ELAPSED_TIME, CREDITS_USED_CLOUD_SERVICES,
                  DATABASE_NAME, SCHEMA_NAME, BYTES_SCANNED, BYTES_WRITTEN, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                WHERE EXECUTION_STATUS = 'SUCCESS'
                  AND START_TIME > %s AND START_TIME <= %s
                  {exclude_username_clause(self._query_log_excluded_usernames)}
                ORDER BY QUERY_ID
                LIMIT {self._query_log_fetch_size} OFFSET %s
                """,
                (
                    start_date,
                    end_date,
                    *self._query_log_excluded_usernames,
                    x * self._query_log_fetch_size,
                ),
            )
            for x in range(batches)
        }

    def _parse_query_logs(self, batch_number: str, query_logs: List[Tuple]) -> None:
        logger.info(f"query logs batch #{batch_number}")
        for (
            query_id,
            username,
            query_text,
            start_time,
            elapsed_time,
            credit,
            default_database,
            default_schema,
            bytes_scanned,
            bytes_written,
            rows_produced,
            rows_inserted,
            rows_updated,
            *access_objects,
        ) in query_logs:
            try:
                sources = (
                    self._parse_accessed_objects(access_objects[0])
                    if len(access_objects) == 2
                    else None
                )
                targets = (
                    self._parse_accessed_objects(access_objects[1])
                    if len(access_objects) == 2
                    else None
                )

                query_log = QueryLog(
                    id=f"{DataPlatform.SNOWFLAKE.name}:{query_id}",
                    query_id=query_id,
                    platform=DataPlatform.SNOWFLAKE,
                    account=self._account,
                    start_time=start_time,
                    duration=safe_float(elapsed_time / 1000.0),
                    cost=safe_float(credit),
                    user_id=username,
                    default_database=default_database,
                    default_schema=default_schema,
                    rows_read=safe_float(rows_produced),
                    rows_written=safe_float(rows_inserted) or safe_float(rows_updated),
                    bytes_read=safe_float(bytes_scanned),
                    bytes_written=safe_float(bytes_written),
                    sources=sources,
                    targets=targets,
                    sql=query_text,
                    sql_hash=md5_digest(query_text.encode("utf-8")),
                )

                self._logs.append(query_log)
            except Exception:
                logger.exception(f"query log processing error, query id: {query_id}")

    @staticmethod
    def _build_tag_string(tag_key: str, tag_value: str) -> str:
        return f"{tag_key}={tag_value}"

    def _init_dataset(
        self,
        database: str,
        schema: str,
        table: str,
        table_type: str,
        comment: str,
        row_count: Optional[int],
        table_bytes: Optional[float],
    ) -> Dataset:
        normalized_name = dataset_normalized_name(database, schema, table)
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=normalized_name, account=self._account, platform=DataPlatform.SNOWFLAKE
        )

        dataset.source_info = SourceInfo(
            main_url=SnowflakeExtractor.build_table_url(self._account, normalized_name)
        )

        dataset.schema = DatasetSchema(
            schema_type=SchemaType.SQL,
            description=comment,
            fields=[],
            sql_schema=SQLSchema(
                materialization=(
                    MaterializationType.VIEW
                    if table_type == SnowflakeTableType.VIEW.value
                    else MaterializationType.TABLE
                )
            ),
        )

        dataset.statistics = to_dataset_statistics(row_count, table_bytes)

        dataset.structure = DatasetStructure(
            database=database, schema=schema, table=table
        )

        return dataset

    @staticmethod
    def build_table_url(account: str, full_name: str) -> str:
        db, schema, table = full_name.upper().split(".")
        return (
            f"https://{account}.snowflakecomputing.com/console#/data/tables/detail?"
            f"databaseName={db}&schemaName={schema}&tableName={table}"
        )

    def _parse_accessed_objects(self, raw_objects: str) -> List[QueriedDataset]:
        objects = parse_raw_as(List[AccessedObject], raw_objects)
        queried_datasets: List[QueriedDataset] = []
        for obj in objects:
            if not obj.objectDomain or obj.objectDomain.upper() not in (
                "TABLE",
                "VIEW",
                "MATERIALIZED VIEW",
            ):
                continue

            table_name = obj.objectName.lower()
            parts = table_name.split(".")
            if len(parts) != 3:
                logger.debug(f"Invalid table name {table_name}, skip")
                continue

            dataset_id = str(
                to_dataset_entity_id(table_name, DataPlatform.SNOWFLAKE, self._account)
            )
            db, schema, table = parts

            queried_datasets.append(
                QueriedDataset(
                    id=dataset_id,
                    database=db,
                    schema=schema,
                    table=table,
                    columns=[col.columnName for col in obj.columns] or None,
                )
            )

        return queried_datasets
