from typing import Collection, Dict, List

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL, Inspector
from sqlalchemy.sql import sqltypes

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_schema_field
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.database.config import GenericDatabaseRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetPlatform,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    ForeignKey,
    MaterializationType,
    SchemaField,
    SchemaType,
    SQLSchema,
    SystemDescription,
)

logger = get_logger()


def get_precision(type_):
    if isinstance(type_, sqltypes.Numeric):
        return float(type_.precision) if type_.precision else None
    return None


class GenericDatabaseExtractor(BaseExtractor):
    """Generic Database metadata extractor"""

    _description = "Generic metadata crawler"
    _platform = Platform.UNKNOWN
    _sqlalchemy_dialect = ""

    @staticmethod
    def from_config_file(config_file: str) -> "GenericDatabaseExtractor":
        return GenericDatabaseExtractor(
            GenericDatabaseRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: GenericDatabaseRunConfig):
        super().__init__(config)
        self._config = config

        self._alternative_host = config.alternative_host
        self._database = config.database

        self._filter = DatasetFilter.from_two_level_dataset_filter(
            config.filter
        ).normalize()
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from host {self._config.host}")

        inspector = GenericDatabaseExtractor.get_inspector(self._get_sqlalchemy_url())
        for schema in self._get_schemas(inspector):
            self._extract_schema(inspector, schema)
        self._extract_foreign_key(inspector)

        return self._datasets.values()

    def _get_sqlalchemy_url(self) -> URL:
        return URL.create(
            self._sqlalchemy_dialect,
            username=self._config.user,
            password=self._config.password,
            host=self._config.host,
            database=self._config.database,
        )

    @staticmethod
    def get_inspector(url: URL) -> Inspector:
        engine = create_engine(url)
        return inspect(engine)

    def _get_schemas(self, inspector: Inspector) -> List[str]:
        return inspector.get_schema_names()

    def _extract_schema(self, inspector: Inspector, schema: str):
        if not self._filter.include_schema_two_level(schema):
            logger.info(f"Skip schema: {schema}")
            return

        for table in inspector.get_table_names(schema):
            self._extract_table(inspector, schema, table)

    def _extract_table(
        self,
        inspector: Inspector,
        schema: str,
        table: str,
        materialization: MaterializationType = MaterializationType.TABLE,
    ):
        if not self._filter.include_table_two_level(schema, table):
            logger.info(f"Skip table: {schema}.{table}")
            return

        table_info = inspector.get_table_comment(table_name=table, schema=schema)
        columns = inspector.get_columns(table_name=table, schema=schema)
        pk_info = inspector.get_pk_constraint(table_name=table, schema=schema)

        table_name = dataset_normalized_name(
            db=self._database, schema=schema, table=table
        )
        table_description = table_info.get("text")

        fields: List[SchemaField] = []

        for column in columns:
            column_name = column.get("name")

            if not column_name:
                continue

            fields.append(
                build_schema_field(
                    column_name=column_name,
                    field_type=str(column.get("type")),
                    description=column.get("comment"),
                    nullable=bool(column.get("nullable")),
                    precision=get_precision(column.get("type")),
                )
            )

        self._datasets[table_name] = Dataset(
            logical_id=DatasetLogicalID(
                name=table_name,
                platform=DataPlatform[self._platform.value],
                account=self._alternative_host or self._config.host,
            ),
            schema=DatasetSchema(
                description=table_description,
                schema_type=SchemaType.SQL,
                fields=fields,
                sql_schema=SQLSchema(
                    materialization=materialization,
                    primary_key=pk_info.get("constrained_columns") or None,
                ),
            ),
            structure=DatasetStructure(
                database=self._database, schema=schema, table=table
            ),
            system_description=(
                SystemDescription(
                    description=table_description,
                    platform=AssetPlatform[self._platform.value],
                )
                if table_description
                else None
            ),
        )

    def _extract_foreign_key(self, inspector: Inspector):
        for dataset in self._datasets.values():
            table = dataset.structure.table
            schema = dataset.structure.schema

            foreign_key_infos = inspector.get_foreign_keys(
                table_name=table, schema=schema
            )

            if not foreign_key_infos:
                continue

            foreign_keys = []
            for info in foreign_key_infos:
                parent_schema = info.get("referred_schema")
                parent_table = info.get("referred_table")

                parent_logical_id = DatasetLogicalID(
                    account=self._alternative_host or self._config.host,
                    name=dataset_normalized_name(
                        schema=parent_schema, table=parent_table
                    ),
                    platform=DataPlatform[self._platform.value],
                )

                constrained_columns = info.get("constrained_columns") or []
                referred_columns = info.get("referred_columns") or []

                if len(constrained_columns) != len(referred_columns):
                    logger.warning(f"Skip foreign key: {info.get('name')}")
                    continue

                foreign_keys.extend(
                    [
                        ForeignKey(
                            field_path=column,
                            parent=parent_logical_id,
                            parent_field=referred_columns[i],
                        )
                        for i, column in enumerate(constrained_columns)
                    ]
                )

            dataset.schema.sql_schema.foreign_key = foreign_keys
