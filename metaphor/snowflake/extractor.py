import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

from metaphor.common.event_util import EventUtil

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
    EntityType,
    MaterializationType,
    MetadataChangeEvent,
    SchemaField,
    SchemaType,
    SQLSchema,
)

from metaphor.common.extractor import BaseExtractor, RunConfig

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class SnowflakeRunConfig(RunConfig):
    account: str
    user: str
    password: str
    database: str


class SnowflakeExtractor(BaseExtractor):
    """Snowflake metadata extractor"""

    @staticmethod
    def config_class():
        return SnowflakeRunConfig

    def __init__(self):
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeExtractor.config_class())

        logger.info(f"Fetching metadata from Snowflake account {config.account}")
        ctx = snowflake.connector.connect(
            account=config.account, user=config.user, password=config.password
        )

        with ctx:
            cursor = ctx.cursor()

            databases = self._fetch_databases(cursor, config.database)
            logger.info(f"Databases: {databases}")

            # TODO: parallel fetching
            for db in databases:
                tables = self._fetch_tables(cursor, db)
                logger.info(f"DB {db} has tables {tables}")

                for schema, name, full_name in tables:
                    dataset = self._datasets[full_name]
                    self._fetch_columns(cursor, schema, name, dataset)
                    self._fetch_ddl(cursor, schema, name, dataset)

        logger.debug(self._datasets)

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    @staticmethod
    def _table_fullname(db: str, schema: str, name: str):
        """The full table name including database, schema and name"""
        return f"{db}.{schema}.{name}".lower()

    @staticmethod
    def _fetch_databases(cursor, initial_database: str) -> List[str]:
        cursor.execute("USE " + initial_database)
        cursor.execute(
            "SELECT database_name FROM information_schema.databases ORDER BY database_name"
        )
        return [db[0] for db in cursor]

    def _fetch_tables(self, cursor, database: str) -> List[Tuple[str, str, str]]:
        cursor.execute("USE " + database)
        cursor.execute(
            "SELECT table_schema, table_name, table_type, COMMENT "
            "FROM information_schema.tables "
            "WHERE table_schema != 'INFORMATION_SCHEMA' "
            "ORDER BY table_schema, table_name"
        )

        tables: List[Tuple[str, str, str]] = []
        for schema, name, table_type, comment in cursor:
            full_name = self._table_fullname(database, schema, name)
            self._datasets[full_name] = self._init_dataset(
                full_name, table_type, comment
            )
            tables.append((schema, name, full_name))

        return tables

    @staticmethod
    def _fetch_columns(cursor, schema: str, name: str, dataset: Dataset) -> None:
        assert dataset.schema is not None and dataset.schema.fields is not None

        cursor.execute(
            "SELECT ordinal_position, column_name, data_type, character_maximum_length, "
            "  numeric_precision, is_nullable, column_default, comment "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (schema, name),
        )
        for column in cursor:
            dataset.schema.fields.append(SnowflakeExtractor._build_field(column))

    @staticmethod
    def _fetch_ddl(cursor, schema: str, name: str, dataset: Dataset) -> None:
        assert dataset.schema is not None and dataset.schema.sql_schema is not None

        try:
            cursor.execute("SELECT get_ddl('table', %s)", f"{schema}.{name}")
            dataset.schema.sql_schema.table_schema = cursor.fetchone()[0]
        except Exception as e:
            logger.error(e)

    @staticmethod
    def _init_dataset(full_name: str, table_type: str, comment: str) -> Dataset:
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID()
        dataset.logical_id.platform = DataPlatform.SNOWFLAKE
        dataset.logical_id.name = full_name

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

        return dataset

    @staticmethod
    def _build_field(column) -> SchemaField:
        field = SchemaField()
        field.field_path = column[1]
        field.native_type = column[2]
        field.nullable = column[5] == "YES"
        field.description = column[7]
        return field
