import math
from datetime import datetime
from typing import (
    Collection,
    Dict,
    Generator,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Set,
    Tuple,
)

try:
    from snowflake.connector.cursor import DictCursor, SnowflakeCursor
    from snowflake.connector.errors import ProgrammingError
except ImportError:
    print("Please install metaphor[snowflake] extra\n")
    raise

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    normalize_full_dataset_name,
    to_dataset_entity_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.query_history import user_id_or_email
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.common.tag_matcher import tag_datasets
from metaphor.common.utils import (
    chunks,
    safe_float,
    start_of_day,
    to_utc_datetime_from_timestamp,
)
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetPlatform,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    EntityType,
    EntityUpstream,
    Hierarchy,
    HierarchyLogicalID,
    QueryLog,
    SchemaField,
    SchemaType,
    SnowflakeStreamInfo,
    SourceInfo,
    SQLSchema,
    SystemDescription,
    SystemTag,
    SystemTags,
    SystemTagSource,
)
from metaphor.snowflake import auth
from metaphor.snowflake.accessed_object import parse_accessed_objects
from metaphor.snowflake.config import SnowflakeConfig
from metaphor.snowflake.utils import (
    DatasetInfo,
    QueryWithParam,
    SnowflakeTableType,
    append_column_system_tag,
    append_dataset_system_tag,
    check_access_history,
    exclude_username_clause,
    fetch_query_history_count,
    str_to_source_type,
    str_to_stream_type,
    table_type_to_materialization_type,
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

    _description = "Snowflake metadata crawler"
    _platform = Platform.REDSHIFT

    @staticmethod
    def from_config_file(config_file: str) -> "SnowflakeExtractor":
        return SnowflakeExtractor(SnowflakeConfig.from_yaml_file(config_file))

    def __init__(self, config: SnowflakeConfig):
        super().__init__(config)
        self._account = normalize_snowflake_account(config.account)
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)
        self._tag_matchers = config.tag_matchers
        self._query_log_excluded_usernames = config.query_log.excluded_usernames
        self._query_log_lookback_days = config.query_log.lookback_days
        self._query_log_fetch_size = config.query_log.fetch_size
        self._query_log_max_query_size = config.query_log.max_query_size
        self._max_concurrency = config.max_concurrency
        self._streams_enabled = config.streams.enabled
        self._streams_count_rows = config.streams.count_rows
        self._config = config

        self._datasets: Dict[str, Dataset] = {}
        self._hierarchies: Dict[str, Hierarchy] = {}

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

            self._fetch_database_comment(cursor)
            for database in databases:
                tables = self._fetch_tables(cursor, database)
                if len(tables) == 0:
                    logger.info(f"Skip empty database {database}")
                    continue

                logger.info(f"Include {len(tables)} tables from {database}")

                self._fetch_columns(cursor, database)
                self._fetch_schemas_comment(cursor, database)

                secure_views = self._fetch_secure_views(cursor)

                try:
                    self._fetch_table_info(
                        tables, database in shared_databases, secure_views
                    )
                except Exception as e:
                    logger.exception(
                        f"Failed to fetch table extra info for '{database}'\n{e}"
                    )

                if self._streams_enabled:
                    for schema in self._fetch_schemas(cursor):
                        self._fetch_streams(cursor, database, schema)

            self._fetch_primary_keys(cursor)
            self._fetch_unique_keys(cursor)
            self._fetch_tags(cursor)

        datasets = list(self._datasets.values())
        tag_datasets(datasets, self._tag_matchers)

        entities: List[ENTITY_TYPES] = []
        entities.extend(datasets)
        entities.extend(self._hierarchies.values())
        return entities

    def collect_query_logs(self) -> Iterator[QueryLog]:
        self._conn = auth.connect(self._config)

        with self._conn:
            if self._query_log_lookback_days > 0:
                yield from self._fetch_query_logs()

    @staticmethod
    def fetch_databases(cursor: SnowflakeCursor) -> List[str]:
        """
        Fetch all databases from Snowflake, including shared/imported databases.
        """
        cursor.execute(
            "SELECT database_name FROM information_schema.databases ORDER BY database_name"
        )
        return [db[0].lower() for db in cursor]

    @staticmethod
    def _fetch_shared_databases(cursor: SnowflakeCursor) -> List[str]:
        cursor.execute(
            "SELECT database_name FROM information_schema.databases WHERE type = 'IMPORTED DATABASE' ORDER BY database_name"
        )
        return [db[0].lower() for db in cursor]

    @staticmethod
    def _fetch_secure_views(cursor: SnowflakeCursor) -> Set[str]:
        cursor.execute(
            "select table_catalog, table_schema, table_name, is_secure from information_schema.views WHERE table_schema != 'INFORMATION_SCHEMA'"
        )
        set_of_secure_views = set()
        for database, schema, table, is_secure in cursor:
            if not is_secure:
                continue
            fullname = f"{database}.{schema}.{table}".lower()
            set_of_secure_views.add(fullname)
        return set_of_secure_views

    FETCH_TABLE_QUERY = """
    SELECT table_catalog, table_schema, table_name, table_type, COMMENT, row_count, bytes, created
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
            logger.exception(f"Invalid or inaccessible database {database_name}")
            return {}

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
            created,
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
                database,
                schema,
                name,
                table_type,
                comment,
                row_count,
                table_bytes,
                created,
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
        self,
        tables: Dict[str, DatasetInfo],
        is_shared_database: bool,
        secure_views: Set[str],
    ) -> None:
        dict_cursor: DictCursor = self._conn.cursor(DictCursor)  # type: ignore

        # Partition table by schema
        schema_tables: Dict[str, List[Tuple[str, DatasetInfo]]] = {}
        for table in tables.items():
            schema_tables.setdefault(table[1].schema, []).append(table)

        for partitioned_tables in schema_tables.values():
            for chunk in chunks(partitioned_tables, TABLE_INFO_FETCH_SIZE):
                try:
                    self._fetch_table_info_internal(
                        dict_cursor, chunk, is_shared_database, secure_views
                    )
                except Exception as error:
                    logger.error(error)

        dict_cursor.close()

    def _fetch_table_info_internal(
        self,
        dict_cursor: DictCursor,
        tables: List[Tuple[str, DatasetInfo]],
        is_shared_database: bool,
        secure_views: Set[str],
    ) -> None:
        queries, params = [], []
        ddl_tables, updated_time_tables = [], []
        for normalized_name, table in tables:
            fullname = to_quoted_identifier([table.database, table.schema, table.name])
            # fetch last_update_time and DDL for tables, and fetch only DDL for views
            if (
                table.type == SnowflakeTableType.BASE_TABLE.value
                and not is_shared_database
                and normalized_name not in secure_views
            ):
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
        logger.debug(f"{query}, params: {params}")

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
                dataset.source_info.last_updated = to_utc_datetime_from_timestamp(
                    timestamp / 1_000_000_000
                )

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

    def _add_hierarchy_system_tag(
        self,
        database: Optional[str],
        object_name: Optional[str],
        object_type: Literal["DATABASE", "SCHEMA"],
        tag_key: str,
        tag_value: str,
    ) -> None:
        """
        Adds a hierarchy system tag (i.e. a database-level or schema-level tag) to MCE.
        If it's a database-level tag, the corresponding hierarchy's logical_id will contain
        only the database name. Otherwise it will have both the database name and schema
        name.
        """
        if not object_name:
            logger.error("Cannot extract tag without object name")
            return

        # Set hierarchy key and logical_id
        path = [DataPlatform.SNOWFLAKE.value]
        if object_type == "DATABASE":
            if not self._filter.include_database(object_name):
                return
            path.extend(
                [object_name.lower()]
            )  # path should be normalized, i.e. should be lowercase
            hierarchy_key = dataset_normalized_name(object_name)
        else:
            if not database:
                logger.error("Cannot extract schema level tag without database name")
                return
            if not self._filter.include_schema(database, object_name):
                return
            path.extend(
                [database.lower(), object_name.lower()]
            )  # path should be normalized, i.e. should be lowercase
            hierarchy_key = dataset_normalized_name(database, object_name)
        logical_id = HierarchyLogicalID(path=path)

        hierarchy = self._hierarchies.setdefault(
            hierarchy_key,
            Hierarchy(
                logical_id=logical_id,
            ),
        )

        system_tag = SystemTag(
            key=tag_key,
            system_tag_source=SystemTagSource.SNOWFLAKE,
            value=tag_value,
        )
        if hierarchy.system_tags:
            hierarchy.system_tags.tags.append(system_tag)
        else:
            hierarchy.system_tags = SystemTags(tags=[system_tag])

    def _fetch_tags(self, cursor: SnowflakeCursor) -> None:
        query = """
        SELECT TAG_NAME, TAG_VALUE, DOMAIN, OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME
        FROM snowflake.account_usage.tag_references
        WHERE DOMAIN in ('TABLE', 'COLUMN', 'DATABASE', 'SCHEMA')
        AND OBJECT_DELETED IS NULL
        ORDER BY case when DOMAIN = 'DATABASE' then 1
                        when DOMAIN = 'SCHEMA' then 2
                        when DOMAIN = 'TABLE' then 3
                        when DOMAIN = 'COLUMN' then 4
                        end asc;
        """
        cursor.execute(query)

        for (
            key,
            value,
            object_type,
            database,
            schema,
            object_name,
            column_name,
        ) in cursor:
            if object_type in {"DATABASE", "SCHEMA"}:
                self._add_hierarchy_system_tag(
                    database, object_name, object_type, key, value
                )

            system_tag = SystemTag(
                key=key, value=value, system_tag_source=SystemTagSource.SNOWFLAKE
            )

            if object_type in {"DATABASE", "SCHEMA"}:
                db = object_name if object_type == "DATABASE" else database
                sch = None if object_type == "DATABASE" else object_name
                prefix = f"{dataset_normalized_name(db=db, schema=sch)}."
                for dataset_key in self._datasets:
                    # Need to make sure key_prefix == `db`.`schema`
                    if dataset_key.startswith(prefix):
                        append_dataset_system_tag(
                            self._datasets[dataset_key], system_tag
                        )

            elif object_type == "TABLE":
                dataset_key = dataset_normalized_name(
                    db=database, schema=schema, table=object_name
                )
                if dataset_key not in self._datasets:
                    logger.warning(
                        f"Ignoring tag {system_tag} for excluded dataset {dataset_key}"
                    )
                else:
                    append_dataset_system_tag(self._datasets[dataset_key], system_tag)

            elif object_type == "COLUMN":
                dataset_key = dataset_normalized_name(
                    db=database, schema=schema, table=object_name
                )
                if dataset_key not in self._datasets:
                    logger.warning(
                        f"Ignoring tag {system_tag} for excluded dataset {dataset_key}"
                    )
                else:
                    append_column_system_tag(
                        self._datasets[dataset_key], system_tag, column_name
                    )
            else:
                logger.error(f"Unknown object_type: {object_type}")

    def _fetch_query_logs(self) -> Iterator[QueryLog]:
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

        cursor = self._conn.cursor()
        parsed_query_log_count = 0
        for batch, query in queries.items():
            cursor.execute(query.query, query.params)
            for query_log in self._parse_query_logs(batch, cursor):
                parsed_query_log_count += 1
                yield query_log

        logger.info(f"Fetched {parsed_query_log_count} query logs")

    def _fetch_schemas(self, cursor: SnowflakeCursor) -> List[str]:
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'INFORMATION_SCHEMA'"
        )
        return [schema[0] for schema in cursor]

    def _fetch_streams(self, cursor: SnowflakeCursor, database: str, schema: str):
        try:
            cursor.execute(f"SHOW STREAMS IN {schema}")
        except Exception:
            # Most likely due to a permission issue
            logger.exception(f"Failed to show streams in '{schema}'")
            return

        count = 0
        for entry in cursor:
            (
                create_on,
                stream_name,
                comment,
                source_name,
                source_type_str,
                stale_,
                stream_type_str,
                stale_after,
            ) = (
                entry[0],
                entry[1],
                entry[5],
                entry[6],
                entry[7],
                entry[10],
                entry[11],
                entry[12],
            )

            row_count = (
                self._fetch_stream_row_count(f"{database}.{schema}.{stream_name}")
                if self._streams_count_rows
                else None
            )

            stale = str(stale_) == "true"

            normalized_name = dataset_normalized_name(database, schema, stream_name)
            dataset = self._init_dataset(
                database=database,
                schema=schema,
                table=stream_name,
                table_type="STREAM",
                comment=comment,
                row_count=row_count,
                table_bytes=None,  # Not applicable to streams
                created=create_on,
            )

            def _to_dataset_eid(x: str) -> str:
                """Lil helper function to save me some keystrokes"""
                return str(
                    to_dataset_entity_id(
                        normalize_full_dataset_name(x),
                        DataPlatform.SNOWFLAKE,
                        self._account,
                    )
                )

            source_entities = [_to_dataset_eid(source_name)]
            dataset.entity_upstream = EntityUpstream(
                source_entities=source_entities,
            )

            source_type = str_to_source_type.get(source_type_str.upper())
            if not source_type:
                logger.warning(f"Unknown source type: {source_type_str.upper()}")
                continue

            stream_type = str_to_stream_type.get(stream_type_str.upper())
            if not stream_type:
                logger.warning(f"Unknown stream type: {stream_type_str.upper()}")
                continue

            dataset.snowflake_stream_info = SnowflakeStreamInfo(
                source_type=source_type,
                stream_type=stream_type,
                stale_after=stale_after,
                stale=stale,
            )

            self._datasets[normalized_name] = dataset
            count += 1

        logger.info(f"Found {count} stream tables in {database}.{schema}")

    def _fetch_stream_row_count(self, stream_name) -> Optional[int]:
        with self._conn.cursor() as cursor:
            try:
                cursor.execute(f"SELECT COUNT(1) FROM {stream_name}")
            except Exception:
                # Many reasons can cause row counting to fail, e.g.
                # stream staleness or dropped base table
                return None

            result = cursor.fetchone()
            if isinstance(result, tuple):
                return result[0]

        return None

    def _batch_query_for_access_logs(
        self, start_date: datetime, end_date: datetime, batches: int
    ):
        return {
            x: QueryWithParam(
                f"""
                SELECT q.QUERY_ID, q.QUERY_PARAMETERIZED_HASH,
                  q.USER_NAME, QUERY_TEXT, START_TIME, TOTAL_ELAPSED_TIME, CREDITS_USED_CLOUD_SERVICES,
                  DATABASE_NAME, SCHEMA_NAME, BYTES_SCANNED, BYTES_WRITTEN, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED,
                  DIRECT_OBJECTS_ACCESSED, OBJECTS_MODIFIED
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                  ON a.QUERY_ID = q.QUERY_ID
                WHERE EXECUTION_STATUS = 'SUCCESS'
                  AND q.START_TIME > %s AND q.START_TIME <= %s
                  AND a.QUERY_START_TIME > %s AND a.QUERY_START_TIME <= %s
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
                SELECT QUERY_ID, QUERY_PARAMETERIZED_HASH,
                  USER_NAME, QUERY_TEXT, START_TIME, TOTAL_ELAPSED_TIME, CREDITS_USED_CLOUD_SERVICES,
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

    def _parse_query_logs(
        self, batch: int, cursor: SnowflakeCursor
    ) -> Generator[QueryLog, None, None]:
        logger.info(f"query logs batch #{batch}")
        for (
            query_id,
            query_hash,
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
        ) in cursor:
            try:
                sources = (
                    parse_accessed_objects(access_objects[0], self._account)
                    if len(access_objects) == 2
                    else None
                )
                targets = (
                    parse_accessed_objects(access_objects[1], self._account)
                    if len(access_objects) == 2
                    else None
                )

                # Skip large queries
                if len(query_text) > self._query_log_max_query_size:
                    continue

                # User IDs can be an email address
                user_id, email = user_id_or_email(username)

                query_log = QueryLog(
                    id=f"{DataPlatform.SNOWFLAKE.name}:{query_id}",
                    query_id=query_id,
                    platform=DataPlatform.SNOWFLAKE,
                    account=self._account,
                    start_time=start_time,
                    duration=safe_float(elapsed_time / 1000.0),
                    cost=safe_float(credit),
                    user_id=user_id,
                    email=email,
                    default_database=default_database,
                    default_schema=default_schema,
                    rows_read=safe_float(rows_produced),
                    rows_written=safe_float(rows_inserted) or safe_float(rows_updated),
                    bytes_read=safe_float(bytes_scanned),
                    bytes_written=safe_float(bytes_written),
                    sources=sources,
                    targets=targets,
                    sql=query_text,
                    sql_hash=query_hash,
                )

                yield query_log
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
        created: Optional[datetime] = None,
    ) -> Dataset:
        normalized_name = dataset_normalized_name(database, schema, table)
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=normalized_name, account=self._account, platform=DataPlatform.SNOWFLAKE
        )

        dataset.source_info = SourceInfo(
            main_url=SnowflakeExtractor.build_table_url(self._account, normalized_name),
            created_at_source=created,
        )

        sql_schema = SQLSchema()
        if table_type in [item.value for item in SnowflakeTableType]:
            sql_schema.materialization = table_type_to_materialization_type[
                SnowflakeTableType(table_type)
            ]
        else:
            logger.warning(f"Unknown table type: {table_type}")

        dataset.schema = DatasetSchema(
            schema_type=SchemaType.SQL,
            description=comment,
            fields=[],
            sql_schema=sql_schema,
        )

        dataset.statistics = to_dataset_statistics(row_count, table_bytes)

        dataset.structure = DatasetStructure(
            database=database, schema=schema, table=table
        )

        dataset.system_tags = SystemTags(tags=[])

        return dataset

    @staticmethod
    def build_table_url(account: str, full_name: str) -> str:
        db, schema, table = full_name.upper().split(".")

        if "." in account or "-" not in account:
            # Definitely legacy account identifier:
            # - If identifier contains ".", it contains a region part
            # - If it doesn't have a "-", means it's the default region
            # Ref: https://docs.snowflake.com/en/user-guide/admin-account-identifier#format-2-legacy-account-locator-in-a-region
            return (
                f"https://{account}.snowflakecomputing.com/console#/data/tables/detail?"
                f"databaseName={db}&schemaName={schema}&tableName={table}"
            )

        # New account identifier with org name and account name.
        # org name can't have dash in it; as for account name while it
        # is not supposed to have dash inside, Snowflake docs says
        # they allow substituting underscores with dashes, so let's
        # allow that here.
        # Ref: https://docs.snowflake.com/en/user-guide/admin-account-identifier#format-1-preferred-account-name-in-your-organization
        org_name, account_name = account.split(
            "-", 1
        )  # split into org_name, account_name
        return (
            f"https://app.snowflake.com/{org_name}/{account_name}/#/data/"
            f"databases/{db}/schemas/{schema}/table/{table}"
        )

    def _add_hierarchy_comment(
        self,
        database: str,
        comment: str,
        schema: Optional[str] = None,
    ) -> None:
        """
        Adds a hierarchy description (i.e. a database-level or schema-level tag) to MCE.
        """

        if not comment:
            return

        # Set hierarchy key and logical_id
        path = [DataPlatform.SNOWFLAKE.value, database.lower()] + (
            [schema.lower()] if schema else []
        )
        logical_id = HierarchyLogicalID(path=path)
        hierarchy_key = dataset_normalized_name(database, schema)

        hierarchy = self._hierarchies.setdefault(
            hierarchy_key,
            Hierarchy(
                logical_id=logical_id,
            ),
        )
        hierarchy.system_description = SystemDescription(
            platform=AssetPlatform.SNOWFLAKE, description=comment
        )

    def _fetch_database_comment(self, cursor: SnowflakeCursor) -> None:
        cursor.execute(
            "SELECT database_name, comment FROM information_schema.databases"
        )

        database_count, comment_count = 0, 0
        for database_name, comment in cursor:
            database_count += 1
            if comment:
                comment_count += 1
                logger.debug(f"Fetched comment for {database_name}")
            self._add_hierarchy_comment(database_name, comment)

        logger.debug(f"Fetched {comment_count}/{database_count} database comment")

    def _fetch_schemas_comment(self, cursor: SnowflakeCursor, database: str) -> None:
        cursor.execute(
            "SELECT catalog_name, schema_name, comment FROM information_schema.schemata WHERE schema_name != 'INFORMATION_SCHEMA'"
        )

        schema_count, comment_count = 0, 0
        for catalog_name, schema, comment in cursor:
            schema_count += 1
            if comment:
                comment_count += 1
                logger.debug(f"Fetched comment for {catalog_name}.{schema}")
            self._add_hierarchy_comment(
                database=catalog_name, schema=schema, comment=comment
            )

        logger.debug(
            f"Fetched {comment_count}/{schema_count} schema comment in database: {database}"
        )
