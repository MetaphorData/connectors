import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

from serde import deserialize

from metaphor.common.event_util import EventUtil

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

from metaphor.common.extractor import BaseExtractor, RunConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ignored_dbs = ["template0", "template1", "rdsadmin"]
_ignored_schemas = [
    "pg_catalog",
    "information_schema",
    "pg_internal",
    "catalog_history",
]


@deserialize
@dataclass
class PostgresqlRunConfig(RunConfig):
    host: str
    database: str
    user: str
    password: str

    port: int = 5432
    redshift: bool = False  # whether the target is redshift or postgresql


class PostgresqlExtractor(BaseExtractor):
    """PostgreSQL metadata extractor"""

    @staticmethod
    def config_class():
        return PostgresqlRunConfig

    def __init__(self):
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self, config: PostgresqlRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, PostgresqlExtractor.config_class())

        platform = DataPlatform.REDSHIFT if config.redshift else DataPlatform.POSTGRESQL
        logger.info(f"Fetching metadata from {platform} host {config.host}")

        conn = await self._connect_database(config, config.database)
        try:
            databases = await self._fetch_databases(conn)
            logger.info(f"Databases: {databases}")
        finally:
            await conn.close()

        for db in databases:
            conn = await self._connect_database(config, db)
            try:
                tables = await self._fetch_tables(conn, platform)
                logger.info(f"DB {db} has tables {tables}")

                # TODO: parallel fetching
                for schema, name, fullname in tables:
                    dataset = self._datasets[fullname]
                    await self._fetch_columns(conn, schema, name, dataset)
                    if not config.redshift:
                        await self._fetch_constraints(conn, schema, name, dataset)
            finally:
                await conn.close()

        logger.debug(self._datasets)

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    @staticmethod
    def _dataset_name(db: str, schema: str, table: str) -> str:
        """The full table name including database, schema and name"""
        return f"{db}.{schema}.{table}".lower()

    @staticmethod
    async def _connect_database(config: PostgresqlRunConfig, database: str):
        logger.info(f"Connecting to DB {database}")
        return await asyncpg.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=database,
        )

    @staticmethod
    async def _fetch_databases(conn) -> List[str]:
        results = await conn.fetch("SELECT datname FROM pg_database")
        return [r[0] for r in results if r[0] not in _ignored_dbs]

    async def _fetch_tables(
        self, conn, platform: DataPlatform
    ) -> List[Tuple[str, str, str]]:
        # e.g. table_schema not in ($1, $2, ...)
        excluded_schemas_clause = f" table_schema NOT IN ({','.join([f'${i + 1}' for i in range(len(_ignored_schemas))])})"

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
            WHERE {excluded_schemas_clause}
            ORDER BY table_schema, table_name;
            """,
            *_ignored_schemas,
        )
        # TODO: the table size query above does NOT work for redshift, use SVV_TABLE_INFO instead [sc3610]
        # https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html

        tables: List[Tuple[str, str, str]] = []
        for table in results:
            catalog = table["table_catalog"]
            schema = table["table_schema"]
            name = table["table_name"]
            table_type = table["table_type"]
            description = table["description"]
            row_count = table["row_count"]
            table_size = table["table_size"]
            full_name = self._dataset_name(catalog, schema, name)
            self._datasets[full_name] = self._init_dataset(
                full_name, table_type, platform, description, row_count, table_size
            )
            tables.append((schema, name, full_name))

        return tables

    @staticmethod
    async def _fetch_columns(conn, schema: str, name: str, dataset: Dataset) -> None:
        assert dataset.schema is not None and dataset.schema.fields is not None

        results = await conn.fetch(
            "SELECT ordinal_position, cols.column_name, data_type, character_maximum_length, "
            "  numeric_precision, is_nullable, pgd.description "
            "FROM information_schema.columns AS cols "
            "LEFT OUTER JOIN pg_catalog.pg_description pgd "
            "  ON pgd.objoid = ( "
            "    SELECT oid FROM pg_class "
            "    WHERE relname = $2 AND relnamespace = ( "
            "        SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1 "
            "      ) "
            "    ) "
            "  AND pgd.objsubid = cols.ordinal_position "
            "WHERE cols.table_schema = $1 AND cols.table_name = $2 "
            "ORDER BY ordinal_position",
            schema,
            name,
        )
        for column in results:
            dataset.schema.fields.append(PostgresqlExtractor._build_field(column))

    @staticmethod
    async def _fetch_constraints(conn, schema: str, name: str, dataset: Dataset):
        assert dataset.schema is not None and dataset.schema.sql_schema is not None

        results = await conn.fetch(
            "SELECT constraints.constraint_name, constraints.constraint_type, "
            "  string_agg(key_col.column_name, ',') AS key_columns, "
            "  constraint_col.table_catalog AS constraint_db, "
            "  constraint_col.table_schema AS constraint_schema, "
            "  constraint_col.table_name AS constraint_table, "
            "  string_agg(constraint_col.column_name, ',') AS constraint_columns "
            "FROM information_schema.table_constraints AS constraints "
            "LEFT OUTER JOIN information_schema.key_column_usage AS key_col "
            "  ON constraints.table_schema = key_col.table_schema "
            "  AND constraints.constraint_name = key_col.constraint_name "
            "LEFT OUTER JOIN  information_schema.constraint_column_usage AS constraint_col "
            "  ON constraints.table_schema = constraint_col.table_schema "
            "  AND constraints.constraint_name = constraint_col.constraint_name "
            "WHERE constraints.table_schema =$1 AND constraints.table_name = $2 "
            "  AND constraints.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY') "
            "GROUP BY constraints.constraint_name, constraints.constraint_type, constraint_col.table_catalog, "
            "  constraint_col.table_schema, constraint_col.table_name",
            schema,
            name,
        )
        if results:
            for constraint in results:
                PostgresqlExtractor._build_constraint(
                    constraint, dataset.schema.sql_schema
                )

    @staticmethod
    def _init_dataset(
        full_name: str,
        table_type: str,
        platform: DataPlatform,
        description: str,
        row_count: int,
        table_size: int,
    ) -> Dataset:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID()
        dataset.logical_id.platform = platform
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

        return dataset

    @staticmethod
    def _build_field(column) -> SchemaField:
        field = SchemaField()
        field.field_path = column["column_name"]
        field.native_type = column["data_type"]
        field.nullable = column["is_nullable"] == "YES"
        field.description = column["description"]
        return field

    @staticmethod
    def _build_constraint(constraint: Dict, schema: SQLSchema) -> None:
        if constraint["constraint_type"] == "PRIMARY KEY":
            schema.primary_key = constraint["key_columns"].split(",")
        elif constraint["constraint_type"] == "FOREIGN KEY":
            foreign_key = ForeignKey()
            foreign_key.field_path = constraint["key_columns"]
            foreign_key.parent = DatasetLogicalID()
            foreign_key.parent.name = PostgresqlExtractor._dataset_name(
                constraint["constraint_db"],
                constraint["constraint_schema"],
                constraint["constraint_table"],
            )
            foreign_key.parent.platform = DataPlatform.POSTGRESQL
            foreign_key.parent_field = constraint["constraint_columns"]

            if not schema.foreign_key:
                schema.foreign_key = []
            schema.foreign_key.append(foreign_key)
