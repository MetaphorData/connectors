from typing import Dict, List

from asyncpg import Connection

from metaphor.common.event_util import EventUtil
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
    MetadataChangeEvent,
    SchemaField,
    SchemaType,
    SQLSchema,
)

from metaphor.common.extractor import BaseExtractor

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

    async def extract(self, config: PostgreSQLRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching metadata from postgreSQL host {config.host}")

        databases = await self._fetch_databases(config)

        for db in databases:
            conn = await self._connect_database(config, db)
            try:
                await self._fetch_tables(conn)
                await self._fetch_columns(conn, db)
                await self._fetch_constraints(conn, db)
            finally:
                await conn.close()

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    @staticmethod
    def _dataset_name(db: str, schema: str, table: str) -> str:
        """The full table name including database, schema and name"""
        return f"{db}.{schema}.{table}".lower()

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

    async def _fetch_tables(self, conn: Connection) -> None:

        results = await conn.fetch(
            f"""
            SELECT table_catalog, table_schema, table_name, table_type, pgd.description,
                pgc.reltuples::bigint AS row_count,
                pg_total_relation_size('"' || table_schema || '"."' || table_name || '"') as table_size
            FROM information_schema.tables t
            JOIN pg_class pgc
              ON pgc.relname = t.table_name
                AND pgc.relnamespace = (
                  SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = t.table_schema
                )
            LEFT JOIN pg_catalog.pg_description pgd
              ON pgd.objoid = pgc.oid AND pgd.objsubid = 0
            WHERE {self._excluded_schemas_clause}
            ORDER BY table_schema, table_name;
            """,
            *_ignored_schemas,
        )

        for table in results:
            catalog = table["table_catalog"]
            schema = table["table_schema"]
            name = table["table_name"]
            table_type = table["table_type"]
            description = table["description"]
            row_count = table["row_count"]
            table_size = table["table_size"]
            full_name = self._dataset_name(catalog, schema, name)
            self._init_dataset(
                full_name, table_type, description, row_count, table_size
            )

    async def _fetch_columns(self, conn: Connection, catalog: str) -> None:
        columns = await conn.fetch(
            f"""
            SELECT table_schema, table_name, ordinal_position, column_name, data_type,
                   character_maximum_length, numeric_precision, is_nullable, pgd.description
            FROM information_schema.columns AS cols
            JOIN (
                    SELECT pgc.oid, relname, nspname
                    FROM pg_class pgc
                    JOIN pg_catalog.pg_namespace pgn
                        ON relnamespace = pgn.oid
                ) pg
                ON table_schema = nspname AND table_name = relname
            LEFT JOIN pg_catalog.pg_description pgd
              ON pgd.objoid = pg.oid AND pgd.objsubid = cols.ordinal_position
            WHERE {self._excluded_schemas_clause}
            ORDER BY table_schema, table_name, ordinal_position;
            """,
            *_ignored_schemas,
        )

        for column in columns:
            full_name = self._dataset_name(
                catalog, column["table_schema"], column["table_name"]
            )
            dataset = self._datasets[full_name]
            assert dataset.schema is not None and dataset.schema.fields is not None

            dataset.schema.fields.append(PostgreSQLExtractor._build_field(column))

    async def _fetch_constraints(self, conn: Connection, catalog: str) -> None:
        constraints = await conn.fetch(
            """
            SELECT constraints.table_schema, constraints.table_name,
              constraints.constraint_name, constraints.constraint_type,
              string_agg(key_col.column_name, ',') AS key_columns,
              constraint_col.table_catalog AS constraint_db,
              constraint_col.table_schema AS constraint_schema,
              constraint_col.table_name AS constraint_table,
              string_agg(constraint_col.column_name, ',') AS constraint_columns
            FROM information_schema.table_constraints AS constraints
            LEFT OUTER JOIN information_schema.key_column_usage AS key_col
              ON constraints.table_schema = key_col.table_schema
              AND constraints.constraint_name = key_col.constraint_name
            LEFT OUTER JOIN  information_schema.constraint_column_usage AS constraint_col
              ON constraints.table_schema = constraint_col.table_schema
              AND constraints.constraint_name = constraint_col.constraint_name
            WHERE constraints.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
            GROUP BY constraints.table_schema, constraints.table_name, constraints.constraint_name,
              constraints.constraint_type, constraint_col.table_catalog, constraint_col.table_schema,
              constraint_col.table_name;
            """
        )

        if not constraints:
            return

        for constraint in constraints:
            full_name = self._dataset_name(
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
    ) -> None:
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

    @staticmethod
    def _build_field(column) -> SchemaField:
        return SchemaField(
            field_path=column["column_name"],
            native_type=column["data_type"],
            nullable=column["is_nullable"] == "YES",
            description=column["description"],
            max_length=float(column["character_maximum_length"])
            if column["character_maximum_length"] is not None
            else None,
            precision=float(column["numeric_precision"])
            if column["numeric_precision"] is not None
            else None,
        )

    @staticmethod
    def _build_constraint(constraint: Dict, schema: SQLSchema) -> None:
        if constraint["constraint_type"] == "PRIMARY KEY":
            schema.primary_key = constraint["key_columns"].split(",")
        elif constraint["constraint_type"] == "FOREIGN KEY":
            foreign_key = ForeignKey()
            foreign_key.field_path = constraint["key_columns"]
            foreign_key.parent = DatasetLogicalID()
            foreign_key.parent.name = PostgreSQLExtractor._dataset_name(
                constraint["constraint_db"],
                constraint["constraint_schema"],
                constraint["constraint_table"],
            )
            foreign_key.parent.platform = DataPlatform.POSTGRESQL
            foreign_key.parent_field = constraint["constraint_columns"]

            if not schema.foreign_key:
                schema.foreign_key = []
            schema.foreign_key.append(foreign_key)
