from typing import Collection, Iterable, List, Tuple

from pyhive import hive

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.hive.config import HiveRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    EntityType,
    MaterializationType,
    SchemaField,
    SchemaType,
    SQLSchema,
)

logger = get_logger()


class HiveExtractor(BaseExtractor):
    """Hive metadata extractor"""

    _description = "Hive metadata crawler"
    _platform = Platform.HIVE

    @staticmethod
    def from_config_file(config_file: str) -> "HiveExtractor":
        return HiveExtractor(HiveRunConfig.from_yaml_file(config_file))

    def __init__(self, config: HiveRunConfig) -> None:
        super().__init__(config)
        self._config = config

    @staticmethod
    def get_connection(**kwargs) -> hive.Connection:
        return hive.connect(**kwargs)

    @staticmethod
    def extract_names_from_cursor(cursor: Iterable[Tuple]) -> List[str]:
        return [tup[0] for tup in cursor]

    def _extract_table(
        self, database: str, table: str, materialization: MaterializationType
    ) -> Dataset:
        with self._connection.cursor() as cursor:
            normalized_name = dataset_normalized_name(schema=database, table=table)
            dataset = Dataset(
                entity_type=EntityType.DATASET,
                logical_id=DatasetLogicalID(
                    name=normalized_name,
                    platform=DataPlatform.HIVE,
                ),
            )
            fields = []
            cursor.execute(f"describe {database}.{table}")
            for field_path, field_type, comment in cursor:
                fields.append(
                    SchemaField(
                        field_path=field_path,
                        native_type=field_type,
                        description=comment if comment else None,
                    )
                )

            cursor.execute(f"show create table {database}.{table}")
            table_schema = "\n".join(
                line for line in HiveExtractor.extract_names_from_cursor(cursor)
            )

            dataset_schema = None
            if fields or table_schema:
                dataset_schema = DatasetSchema()
                if fields:
                    dataset_schema.fields = fields
                if table_schema:
                    dataset_schema.schema_type = SchemaType.SQL
                    dataset_schema.sql_schema = SQLSchema(
                        materialization=materialization,
                        table_schema=table_schema,
                    )
            dataset.schema = dataset_schema

            return dataset

    def _extract_database(self, database: str) -> List[Dataset]:
        with self._connection.cursor() as cursor:
            datasets = []
            cursor.execute(f"show tables in {database}")
            for materialized_view in HiveExtractor.extract_names_from_cursor(cursor):
                datasets.append(
                    self._extract_table(
                        database, materialized_view, MaterializationType.TABLE
                    )
                )

            cursor.execute(f"show views in {database}")
            for materialized_view in HiveExtractor.extract_names_from_cursor(cursor):
                datasets.append(
                    self._extract_table(
                        database, materialized_view, MaterializationType.VIEW
                    )
                )

            cursor.execute(f"show materialized views in {database}")
            for materialized_view in HiveExtractor.extract_names_from_cursor(cursor):
                datasets.append(
                    self._extract_table(
                        database,
                        materialized_view,
                        MaterializationType.MATERIALIZED_VIEW,
                    )
                )

            return datasets

    async def extract(self) -> Collection[ENTITY_TYPES]:
        self._connection = HiveExtractor.get_connection(**self._config.connect_kwargs)
        entities: List[ENTITY_TYPES] = []
        with self._connection.cursor() as cursor:
            cursor.execute("show databases")
            for database in HiveExtractor.extract_names_from_cursor(cursor):
                entities.extend(self._extract_database(database))
        return entities
