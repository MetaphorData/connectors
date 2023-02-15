from typing import Collection, Dict, List

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL, Inspector

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
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
)
from metaphor.mysql.config import MySQLRunConfig

logger = get_logger()


class MySQLExtractor(BaseExtractor):
    """MySQL metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "MySQLExtractor":
        return MySQLExtractor(MySQLRunConfig.from_yaml_file(config_file))

    def __init__(self, config: MySQLRunConfig):
        super().__init__(config, "MySQL metadata crawler", Platform.MYSQL)
        self._config = config
        self._filter = DatasetFilter.from_two_level_dataset_filter(
            config.filter
        ).normalize()
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from MySQL host {self._config.host}")

        inspector = MySQLExtractor.get_inspector(self.get_sqlalchemy_url())
        for schema in self.get_schemas(inspector):
            self.extract_schema(inspector, schema)
        self.extract_foreign_key(inspector)

        return self._datasets.values()

    def get_sqlalchemy_url(self) -> str:
        return URL.create(
            "mysql+pymysql",
            username=self._config.user,
            password=self._config.password,
            host=self._config.host,
            database=self._config.database,
        )

    @staticmethod
    def get_inspector(url: str) -> Inspector:
        engine = create_engine(url)
        return inspect(engine)

    def get_schemas(self, inspector: Inspector) -> List[str]:
        return inspector.get_schema_names()

    def extract_schema(self, inspector: Inspector, schema: str):
        if not self._filter.include_schema_two_level(schema):
            logger.info(f"Skip schema: {schema}")
            return

        for table in inspector.get_table_names(schema):
            self.extract_table(inspector, schema, table)

    def extract_table(self, inspector: Inspector, schema: str, table: str):
        if not self._filter.include_table_two_level(schema, table):
            logger.info(f"Skip table: {schema}.{table}")
            return

        table_info = inspector.get_table_comment(table_name=table, schema=schema)
        columns = inspector.get_columns(table_name=table, schema=schema)
        pk_info = inspector.get_pk_constraint(table_name=table, schema=schema)

        table_name = dataset_normalized_name(schema=schema, table=table)
        self._datasets[table_name] = Dataset(
            logical_id=DatasetLogicalID(
                name=table_name, platform=DataPlatform.MYSQL, account=self._config.host
            ),
            schema=DatasetSchema(
                description=table_info.get("text"),
                schema_type=SchemaType.SQL,
                fields=[
                    SchemaField(
                        description=column.get("comment"),
                        field_name=column.get("name"),
                        field_path=column.get("name"),
                        native_type=str(column.get("type")),
                        nullable=bool(column.get("nullable")),
                        subfields=None,
                    )
                    for column in columns
                ],
                sql_schema=SQLSchema(
                    materialization=MaterializationType.TABLE,
                    primary_key=pk_info.get("constrained_columns"),
                ),
            ),
            structure=DatasetStructure(schema=schema, table=table),
        )

    def extract_foreign_key(self, inspector: Inspector):
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
                    account=self._config.host,
                    name=dataset_normalized_name(
                        schema=parent_schema, table=parent_table
                    ),
                    platform=DataPlatform.MYSQL,
                )

                constrained_columns = info.get("constrained_columns")
                referred_columns = info.get("referred_columns")

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
