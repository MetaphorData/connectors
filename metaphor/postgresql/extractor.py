from typing import Callable, Collection, Dict, List, Optional, Tuple

from asyncpg import Connection

from metaphor.common.entity_id import dataset_fullname
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.postgresql.config import PostgreSQLRunConfig

try:
    import asyncpg
except ImportError:
    print("Please install metaphor[postgresql] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    ForeignKey,
    MaterializationType,
    SchemaField,
    SchemaType,
    SQLSchema,
)

from metaphor.common.extractor import BaseExtractor
from metaphor.common.filter import DatasetFilter

logger = get_logger(__name__)

_ignored_dbs = ["template0", "template1", "rdsadmin"]
_ignored_schemas = [
    "pg_catalog",
    "information_schema",
    "pg_internal",
    "catalog_history",
]


class PostgreSQLExtractor(BaseExtractor):
    """PostgreSQL metadata extractor"""

    @staticmethod
    def config_class():
        return PostgreSQLRunConfig

    def __init__(self):
        self._platform = DataPlatform.POSTGRESQL
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self, config: PostgreSQLRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching metadata from postgreSQL host {config.host}")

        filter = config.filter.normalize()

        databases = (
            await self._fetch_databases(config)
            if filter.includes is None
            else list(filter.includes.keys())
        )

        for db in databases:
            conn = await self._connect_database(config, db)
            try:
                await self._fetch_tables(conn, db, filter)
                await self._fetch_columns(conn, db, filter)
                await self._fetch_constraints(conn, db)
            finally:
                await conn.close()

        return self._datasets.values()

    @staticmethod
    async def _connect_database(
        config: PostgreSQLRunConfig, database: str
    ) -> Connection:
        logger.info(f"Connecting to DB {database}")
        return await asyncpg.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=database,
        )

    @staticmethod
    async def _fetch_databases(config: PostgreSQLRunConfig) -> List[str]:
        conn = await PostgreSQLExtractor._connect_database(config, config.database)
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
        self, conn: Connection, database: str, filter: DatasetFilter
    ) -> List[Dataset]:
        results = await conn.fetch(
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
            ORDER BY schemaname, name;
            """
        )

        datasets = []

        for table in results:
            schema = table["schemaname"]
            name = table["name"]
            description = table["description"]
            row_count = table["row_count"]
            table_size = table["table_size"]
            table_type = table["table_type"]
            full_name = dataset_fullname(database, schema, name)
            if not filter.include_table(database, schema, name):
                logger.info(f"Ignore {full_name} due to filter config")
                continue
            datasets.append(
                self._init_dataset(
                    full_name, table_type, description, row_count, table_size
                )
            )

        return datasets

    async def _fetch_columns(
        self, conn: Connection, catalog: str, filter: DatasetFilter
    ) -> List[Dataset]:
        columns = await conn.fetch(
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
            ORDER BY nc.nspname, c.relname, a.attnum
            """
        )

        datasets = []
        seen = set()

        for column in columns:
            schema, name = column["table_schema"], column["table_name"]
            full_name = dataset_fullname(catalog, schema, name)
            if not filter.include_table(catalog, schema, name):
                logger.info(f"Ignore {full_name} due to filter config")
                continue
            dataset = self._datasets[full_name]
            assert dataset.schema is not None and dataset.schema.fields is not None

            dataset.schema.fields.append(PostgreSQLExtractor._build_field(column))
            if full_name not in seen:
                datasets.append(dataset)
                seen.add(full_name)

        return datasets

    async def _fetch_constraints(self, conn: Connection, catalog: str) -> None:
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
            full_name = dataset_fullname(
                catalog, constraint["table_schema"], constraint["table_name"]
            )
            if full_name not in self._datasets:
                logger.warning(f"Table {full_name} not found")
                continue

            dataset = self._datasets[full_name]
            assert dataset.schema is not None and dataset.schema.sql_schema is not None

            self._build_constraint(constraint, dataset.schema.sql_schema)

    def _init_dataset(
        self,
        full_name: str,
        table_type: str,
        description: str,
        row_count: int,
        table_size: int,
    ) -> Dataset:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID()
        dataset.logical_id.platform = self._platform
        dataset.logical_id.name = full_name

        dataset.schema = DatasetSchema()
        dataset.schema.schema_type = SchemaType.SQL
        dataset.schema.description = description
        dataset.schema.fields = []
        dataset.schema.sql_schema = SQLSchema()
        dataset.schema.sql_schema.materialization = (
            MaterializationType.VIEW
            if table_type == "VIEW"
            else MaterializationType.TABLE
        )

        dataset.statistics = DatasetStatistics()
        dataset.statistics.record_count = float(row_count)
        dataset.statistics.data_size = table_size / (1000 * 1000)  # in MB
        # There is no reliable way to directly get data last modified time, can explore alternatives in future
        # https://dba.stackexchange.com/questions/58214/getting-last-modification-date-of-a-postgresql-database-table/168752

        self._datasets[full_name] = dataset

        return dataset

    @staticmethod
    def _build_field(column) -> SchemaField:
        native_type = column["data_type"]
        precision, max_length = PostgreSQLExtractor._parse_format_type(
            native_type, column["format"]
        )

        return SchemaField(
            field_path=column["column_name"],
            native_type=native_type,
            nullable=(not column["not_null"]),
            description=column["description"],
            max_length=max_length,
            precision=precision,
        )

    @staticmethod
    def _build_constraint(constraint: Dict, schema: SQLSchema) -> None:
        if constraint["constraint_type"] == "PRIMARY KEY":
            schema.primary_key = constraint["key_columns"].split(",")
        elif constraint["constraint_type"] == "FOREIGN KEY":
            foreign_key = ForeignKey()
            foreign_key.field_path = constraint["key_columns"]
            foreign_key.parent = DatasetLogicalID()
            foreign_key.parent.name = dataset_fullname(
                constraint["constraint_db"],
                constraint["constraint_schema"],
                constraint["constraint_table"],
            )
            foreign_key.parent.platform = DataPlatform.POSTGRESQL
            foreign_key.parent_field = constraint["constraint_columns"]

            if not schema.foreign_key:
                schema.foreign_key = []
            schema.foreign_key.append(foreign_key)

    @staticmethod
    def _safe_parse(
        type_str: str, parse: Callable[[str], str], name: str
    ) -> Optional[float]:
        try:
            field = parse(type_str)
            return float(field)
        except IndexError:
            logger.warning(f"Failed to parse {name} from {type_str}.")
        except ValueError:
            logger.warning(f"Failed to convert {field} to float, type_str: {type_str}")
        return None

    @staticmethod
    def _parse_precision(type_str: str) -> Optional[float]:
        def extract(type_str: str) -> str:
            return type_str.split("(")[1].split(",")[0]

        return PostgreSQLExtractor._safe_parse(type_str, extract, "precision")

    @staticmethod
    def _parse_max_length(type_str: str) -> Optional[float]:
        def extract(type_str: str) -> str:
            return type_str.split("(")[1].strip(")")

        return PostgreSQLExtractor._safe_parse(type_str, extract, "max_length")

    @staticmethod
    def _parse_format_type(
        native_type: str,
        type_str: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        precision, max_length = None, None

        excluded_types = (
            "timestamp with time zone",
            "timestamp without time zone",
            "boolean",
            "date",
            "text",
        )

        if native_type in excluded_types:
            return precision, max_length

        if native_type == "integer":
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
        elif native_type == "character varying" or native_type == "character":
            max_length = PostgreSQLExtractor._parse_max_length(type_str)
        else:
            logger.warning(f"Not supported native type: {native_type}")

        return precision, max_length
