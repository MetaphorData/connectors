import re
from datetime import datetime, timezone
from typing import Collection, Dict, Iterator, List, Optional, Tuple

try:
    import asyncpg
    import boto3
except ImportError:
    print("Please install metaphor[postgresql] extra\n")
    raise

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_schema_field
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.sql.process_query.process_query import process_query
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.sql.utils import is_valid_queried_datasets
from metaphor.common.utils import md5_digest, safe_float, start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    ForeignKey,
    MaterializationType,
    QueryLog,
    SchemaField,
    SchemaType,
    SQLSchema,
)
from metaphor.postgresql.config import (
    BasePostgreSQLRunConfig,
    PostgreSQLQueryLogConfig,
    PostgreSQLRunConfig,
)
from metaphor.postgresql.log_parser import ParsedLog, parse_postgres_log

logger = get_logger()

_ignored_dbs = ["template0", "template1", "rdsadmin"]
_ignored_schemas = [
    "pg_catalog",
    "information_schema",
    "pg_internal",
    "catalog_history",
]

LOG_PREFIX_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


class BasePostgreSQLExtractor(BaseExtractor):
    _description = "PostgreSQL metadata crawler"
    _platform = Platform.POSTGRESQL
    _dataset_platform = DataPlatform.POSTGRESQL

    def __init__(self, config: BasePostgreSQLRunConfig):
        super().__init__(config)
        self._host = config.host
        self._database = config.database
        self._user = config.user
        self._password = config.password
        self._filter = config.filter.normalize()
        self._port = config.port

        self._query_log_config = config.query_log

        self._datasets: Dict[str, Dataset] = {}

    async def _connect_database(self, database: str) -> asyncpg.Connection:
        logger.info(f"Connecting to DB {database}")
        return await asyncpg.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=database,
        )

    async def _fetch_databases(self) -> List[str]:
        conn = await self._connect_database(self._database)
        try:
            results = await conn.fetch("SELECT datname FROM pg_database")
            databases = [r[0] for r in results if r[0] not in _ignored_dbs]
            logger.info(f"Databases: {databases}")
            return databases
        finally:
            await conn.close()

    # Exclude schemas in WHERE clause, e.g. table_schema not in ($1, $2, ...)
    _excluded_schemas_clause = f" table_schema NOT IN ({','.join([f'${i + 1}' for i in range(len(_ignored_schemas))])})"

    async def _fetch_tables(
        self,
        conn: asyncpg.Connection,
        catalog: str,
        redshift: bool = False,
    ) -> List[Dataset]:
        parts = [
            """
            SELECT schemaname, tablename AS name, pgd.description, pgc.reltuples::bigint AS row_count,
                pg_total_relation_size(pgc.oid) AS table_size,
                'TABLE' as table_type
            FROM pg_catalog.pg_tables t
            LEFT JOIN pg_class pgc
              ON pgc.relname = t.tablename
            LEFT JOIN pg_catalog.pg_description pgd
              ON pgd.objoid = pgc.oid AND pgd.objsubid = 0
            WHERE schemaname !~ '^pg_' AND schemaname != 'information_schema'
            UNION
            SELECT schemaname, viewname AS name, pgd.description, pgc.reltuples::bigint AS row_count,
                pg_total_relation_size(pgc.oid) AS table_size,
                'VIEW' as table_type
            FROM pg_catalog.pg_views v
            LEFT JOIN pg_class pgc
              ON pgc.relname = v.viewname
            LEFT JOIN pg_catalog.pg_description pgd
              ON pgd.objoid = pgc.oid AND pgd.objsubid = 0
            WHERE schemaname != 'information_schema' and schemaname !~ '^pg_'
            """
        ]
        if redshift:
            parts.append(
                """
            UNION
            SELECT schemaname, tablename AS name, null as description, null AS row_count,
                null AS table_size, 'EXTERNAL' as table_type
            FROM
                pg_catalog.svv_external_tables
            """
            )
        parts.append("ORDER BY schemaname, name;")
        results = await conn.fetch("\n".join(parts))

        datasets = []

        for table in results:
            schema = table["schemaname"]
            name = table["name"]
            description = table["description"]
            row_count = table["row_count"]
            table_size = table["table_size"]
            table_type = table["table_type"]
            if not self._filter.include_table(catalog, schema, name):
                normalized_name = dataset_normalized_name(catalog, schema, name)
                logger.info(f"Ignore {normalized_name} due to filter config")
                continue
            datasets.append(
                self._init_dataset(
                    database=catalog,
                    schema=schema,
                    table=name,
                    table_type=table_type,
                    description=description,
                    row_count=row_count,
                    table_size=table_size,
                )
            )

        return datasets

    async def _fetch_columns(
        self,
        conn: asyncpg.Connection,
        catalog: str,
        redshift: bool = False,
    ) -> List[Dataset]:
        parts = [
            """
            SELECT nc.nspname AS table_schema, c.relname AS table_name,
                a.attnum AS ordinal_position, a.attname AS column_name,
                a.attnotnull AS not_null, format_type(a.atttypid, a.atttypmod) AS format,
                CASE WHEN t.typtype = 'd' THEN
                  CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
                        WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, null)
                        ELSE 'USER-DEFINED' END
                ELSE
                  CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                        WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
                        ELSE 'USER-DEFINED' END
                END AS data_type,
                pgd.description
            FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
                 JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
                 JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
                 LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
                   ON (t.typtype = 'd' AND t.typbasetype = bt.oid)
                 LEFT JOIN pg_catalog.pg_description pgd
                   ON pgd.objoid = c.oid AND pgd.objsubid = a.attnum
            WHERE
              c.relkind in ('r', 'v', 'f', 'p')
              AND nc.nspname != 'information_schema' and nc.nspname !~ '^pg_'
              AND a.attnum > 0
              AND NOT a.attisdropped
            """
        ]
        if redshift:
            parts.append(
                """
            UNION
            SELECT schemaname as table_schema, tablename as table_name, null as ordinal_position,
                columnname as column_name, null as not_null,
                external_type as format, external_type as data_type,
                null as description
            FROM
                pg_catalog.svv_external_columns
            """
            )
        parts.append("ORDER BY table_schema, table_name, ordinal_position;")
        columns = await conn.fetch("\n".join(parts))

        datasets = []
        seen = set()

        for column in columns:
            schema, name = column["table_schema"], column["table_name"]
            normalized_name = dataset_normalized_name(catalog, schema, name)
            if not self._filter.include_table(catalog, schema, name):
                logger.info(f"Ignore {normalized_name} due to filter config")
                continue
            dataset = self._datasets[normalized_name]
            assert dataset.schema is not None and dataset.schema.fields is not None

            dataset.schema.fields.append(PostgreSQLExtractor._build_field(column))
            if normalized_name not in seen:
                datasets.append(dataset)
                seen.add(normalized_name)

        # Fetch view definitions
        # See https://www.postgresql.org/docs/current/view-pg-views.html
        view_definitions = await conn.fetch(
            """
            SELECT *
            FROM pg_catalog.pg_views
            WHERE schemaname != 'information_schema' and schemaname !~ '^pg_';
            """
        )
        for view_definition in view_definitions:
            schema = view_definition["schemaname"]
            name = view_definition["viewname"]
            normalized_name = dataset_normalized_name(catalog, schema, name)
            if not self._filter.include_table(catalog, schema, name):
                continue

            dataset = self._datasets.get(normalized_name)
            if dataset is None:
                logger.warning(f"view {normalized_name} not found")
                continue

            dataset.schema.sql_schema.table_schema = view_definition["definition"]

        return datasets

    async def _fetch_constraints(self, conn: asyncpg.Connection, catalog: str) -> None:
        constraints = await conn.fetch(
            """
            SELECT nr.nspname AS table_schema, r.relname AS table_name,
                   c.conname AS constraint_name,
                   CASE c.contype WHEN 'f' THEN 'FOREIGN KEY'
                                    WHEN 'p' THEN 'PRIMARY KEY'
                                    WHEN 'u' THEN 'UNIQUE' END
                   AS constraint_type,
                   string_agg(a.attname, ',') AS key_columns,
                   string_agg(af.attname, ',') AS constraint_columns,
                   current_database() AS constraint_db,
                   nf.nspname AS constraint_schema, rf.relname AS constraint_table
            FROM (
                SELECT cc.conname, unnest(cc.conkey) AS conkey_id, unnest(cc.confkey) AS confkey_id,
                       cc.connamespace, cc.conrelid, cc.confrelid, cc.contype
                FROM pg_constraint cc
            ) AS c
            JOIN pg_namespace nc ON c.connamespace = nc.oid
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace nr ON nr.oid = r.relnamespace
            LEFT OUTER JOIN pg_attribute a ON a.attrelid = c.conrelid AND c.conkey_id = a.attnum
            LEFT OUTER JOIN pg_attribute af ON af.attrelid = c.confrelid AND c.confkey_id = af.attnum
            LEFT JOIN pg_class rf ON rf.oid = af.attrelid
            LEFT JOIN pg_namespace nf ON nf.oid = rf.relnamespace
            WHERE c.contype IN ('f', 'p', 'u')
                  AND r.relkind IN ('r', 'p')
                  AND (NOT pg_is_other_temp_schema(nr.oid))
            GROUP BY table_schema, table_name, constraint_name, constraint_type,
                     constraint_db, constraint_schema, constraint_table;
            """
        )

        if not constraints:
            return

        for constraint in constraints:
            normalized_name = dataset_normalized_name(
                catalog, constraint["table_schema"], constraint["table_name"]
            )
            if normalized_name not in self._datasets:
                logger.warning(f"Table {normalized_name} not found")
                continue

            dataset = self._datasets[normalized_name]
            assert dataset.schema is not None and dataset.schema.sql_schema is not None

            self._build_constraint(constraint, dataset.schema.sql_schema)

    def _init_dataset(
        self,
        database: str,
        schema: str,
        table: str,
        table_type: str,
        description: str,
        row_count: int,
        table_size: int,
    ) -> Dataset:
        normalized_name = dataset_normalized_name(database, schema, table)

        # There is no reliable way to directly get data last modified time, can explore alternatives in future
        # https://dba.stackexchange.com/questions/58214/getting-last-modification-date-of-a-postgresql-database-table/168752
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID()
        dataset.logical_id.platform = self._dataset_platform
        dataset.logical_id.name = normalized_name

        dataset.schema = DatasetSchema()
        dataset.schema.schema_type = SchemaType.SQL
        dataset.schema.description = description
        dataset.schema.fields = []
        dataset.schema.sql_schema = SQLSchema()
        dataset.schema.sql_schema.materialization = (
            MaterializationType.VIEW
            if table_type == "VIEW"
            else (
                MaterializationType.EXTERNAL
                if table_type == "EXTERNAL"
                else MaterializationType.TABLE
            )
        )

        dataset.statistics = to_dataset_statistics(row_count, table_size)

        dataset.structure = DatasetStructure(
            database=database, schema=schema, table=table
        )

        self._datasets[normalized_name] = dataset

        return dataset

    @staticmethod
    def _build_constraint(constraint: Dict, schema: SQLSchema) -> None:
        if constraint["constraint_type"] == "PRIMARY KEY":
            schema.primary_key = constraint["key_columns"].split(",")
        elif constraint["constraint_type"] == "FOREIGN KEY":
            foreign_key = ForeignKey()
            foreign_key.field_path = constraint["key_columns"]
            foreign_key.parent = DatasetLogicalID()
            foreign_key.parent.name = dataset_normalized_name(
                constraint["constraint_db"],
                constraint["constraint_schema"],
                constraint["constraint_table"],
            )
            foreign_key.parent.platform = DataPlatform.POSTGRESQL
            foreign_key.parent_field = constraint["constraint_columns"]

            if not schema.foreign_key:
                schema.foreign_key = []
            schema.foreign_key.append(foreign_key)


class PostgreSQLExtractor(BasePostgreSQLExtractor):
    """PostgreSQL metadata extractor"""

    _description = "PostgreSQL metadata crawler"
    _platform = Platform.POSTGRESQL
    _dataset_platform = DataPlatform.POSTGRESQL

    @staticmethod
    def from_config_file(config_file: str) -> "PostgreSQLExtractor":
        return PostgreSQLExtractor(PostgreSQLRunConfig.from_yaml_file(config_file))

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from postgreSQL host {self._host}")

        databases = (
            await self._fetch_databases()
            if self._filter.includes is None
            else list(self._filter.includes.keys())
        )

        for db in databases:
            conn = await self._connect_database(db)
            try:
                await self._fetch_tables(conn, db)
                await self._fetch_columns(conn, db)
                await self._fetch_constraints(conn, db)
            finally:
                await conn.close()

        return self._datasets.values()

    def collect_query_logs(self) -> Iterator[QueryLog]:
        if (
            isinstance(self._query_log_config, PostgreSQLQueryLogConfig)
            and self._query_log_config.aws
            and self._query_log_config.lookback_days > 0
            and self._query_log_config.logs_group is not None
        ):
            yield from self._collect_query_logs_from_cloud_watch(
                client=self._query_log_config.aws.get_session().client("logs"),
                lookback_days=self._query_log_config.lookback_days,
                logs_group=self._query_log_config.logs_group,
            )

    def _process_cloud_watch_log(
        self, log: str, previous_line_cache: Dict[str, ParsedLog]
    ) -> Optional[QueryLog]:
        """
        SQL statement and duration are in two consecutive record, example:

        2024...:root@database:[session]:LOG:  statement: SELECT ......
        2024...:root@database:[session]:LOG:  duration: 0.5 ms
        """

        if not LOG_PREFIX_REGEX.match(log):
            """
            insert into can split into 3 consecutive log
            2024...:root@database:[session]:LOG:  statement: INSERT INTO ......
                                                             (a, b, c)
            2024...:root@database:[session]:LOG:  duration: 0.5 ms
            """
            logger.warning("log format is not start with date")
            return None

        parsed = parse_postgres_log(log)

        # Skip log without user and database, and log_level of SQL statement is "LOG"
        if (
            parsed is None
            or not parsed.user
            or not parsed.database
            or parsed.log_level != "LOG"
            or parsed.user in self._query_log_config.excluded_usernames
        ):
            return None

        previous_line = previous_line_cache.get(parsed.session)
        previous_line_cache[parsed.session] = parsed

        # The second line must be: duration: <number> ms
        # Skip log that don't have previous line or invalid log
        if (
            not parsed.log_body[0].lstrip().startswith("duration")
            or not previous_line
            or len(previous_line.log_body) < 2
        ):
            return None

        message_type = previous_line.log_body[0].lstrip()

        # Only `statement` (simple query), and `execute` (extended query) we should care about
        if not message_type.startswith("statement") or message_type.startswith(
            "execute"
        ):
            return None

        # Extract sql from the previous line
        query = ":".join(previous_line.log_body[1:]).lstrip()

        # Extract duration from the current line
        duration = self._extract_duration(parsed.log_body[1])

        tll = extract_table_level_lineage(
            sql=query,
            platform=DataPlatform.POSTGRESQL,
            account=None,
            default_database=parsed.database,
        )

        # Skip if parsed sources or targets has invalid data.
        if not is_valid_queried_datasets(tll.sources) or not is_valid_queried_datasets(
            tll.targets
        ):
            return None

        sql_hash = md5_digest(query.encode("utf-8"))
        sql: Optional[str] = query

        if self._query_log_config.process_query.should_process:
            sql = process_query(
                query,
                DataPlatform.POSTGRESQL,
                self._query_log_config.process_query,
                sql_hash,
            )

        if sql:
            sql_hash = md5_digest(sql.encode("utf-8"))
            return QueryLog(
                id=f"{DataPlatform.POSTGRESQL.name}:{sql_hash}",
                query_id=sql_hash,
                platform=DataPlatform.POSTGRESQL,
                default_database=parsed.database,
                user_id=parsed.user,
                sql=sql,
                sql_hash=sql_hash,
                duration=duration,
                start_time=previous_line.log_time,
                sources=tll.sources,
                targets=tll.targets,
            )
        return None

    def _collect_query_logs_from_cloud_watch(
        self, client: boto3.client, lookback_days: int, logs_group: str
    ) -> Iterator[QueryLog]:

        logger.info(f"Collecting query log from cloud watch for {lookback_days} days")

        # Start time in milliseconds since epoch
        start_timestamp_ms = int(start_of_day(lookback_days).timestamp() * 1000)
        # End time in milliseconds since epoch
        end_timestamp_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        next_token = None
        previous_line_cache: Dict[str, ParsedLog] = {}
        count = 0

        while True:
            params = {
                "logGroupName": logs_group,
                "startTime": start_timestamp_ms,
                "endTime": end_timestamp_ms,
            }
            if next_token:
                params["nextToken"] = next_token
            response = client.filter_log_events(**params)

            next_token = response["nextToken"] if "nextToken" in response else None

            for event in response["events"]:
                message = event["message"]
                count += 1

                query_log = self._process_cloud_watch_log(message, previous_line_cache)
                if query_log:
                    yield query_log

                if count % 1000 == 0:
                    logger.info(f"Processed {count} logs")

            if next_token is None:
                break

    @staticmethod
    def _build_field(column) -> SchemaField:
        native_type = column["data_type"]
        precision, max_length = PostgreSQLExtractor._parse_format_type(
            native_type, column["format"]
        )

        field = build_schema_field(
            column["column_name"],
            native_type,
            column["description"],
            not column["not_null"],
        )
        field.max_length = max_length
        field.precision = precision
        return field

    @staticmethod
    def _parse_precision(type_str: str) -> Optional[float]:
        pattern = re.compile(r"\((\d+)(\,\d+)*\)")
        match = pattern.search(type_str)
        if match and len(match.groups()) >= 1:
            return safe_float(match.group(1))
        return None

    @staticmethod
    def _parse_max_length(type_str: str) -> Optional[float]:
        pattern = re.compile(r"\((\d+)\)")
        match = pattern.search(type_str)
        if match and len(match.groups()) == 1:
            return safe_float(match.group(1))
        return None

    @staticmethod
    def _parse_format_type(
        native_type: str,
        type_str: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        precision, max_length = None, None

        known_types = (
            "time without time zone",
            "time with time zone",
            "interval",
            "uuid",
            "json",
            "jsonb",
            "xml",
            "bytea",
            "ARRAY",
            "USER-DEFINED",
            "point",
            "inet",
            "macaddr",
            "tsquery",
            "tsvector",
        )

        if native_type == "integer" or native_type == "int":
            precision = 32.0
        elif native_type == "smallint":
            precision = 16.0
        elif native_type == "bigint":
            precision = 64.0
        elif native_type == "real":
            precision = 24.0
        elif native_type == "double precision":
            precision = 53.0
        elif native_type == "numeric" and type_str != "numeric":
            precision = PostgreSQLExtractor._parse_precision(type_str)
        elif native_type.startswith("decimal") and type_str != "decimal":
            precision = PostgreSQLExtractor._parse_precision(type_str)
        elif native_type == "character varying" or native_type == "character":
            max_length = PostgreSQLExtractor._parse_max_length(type_str)
        elif native_type not in known_types:
            logger.debug(
                f"Not parsing precision and length for unknown type: {native_type}"
            )

        return precision, max_length

    @staticmethod
    def _extract_duration(log_part: str) -> Optional[float]:
        if log_part.endswith("ms"):
            return float(log_part[:-2])
        return None
