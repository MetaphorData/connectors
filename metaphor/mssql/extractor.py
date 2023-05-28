import json
from typing import Collection, Dict, List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    CustomMetadata,
    CustomMetadataItem,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    ForeignKey,
    MaterializationType,
    SchemaField,
    SQLSchema,
)
from metaphor.mssql.config import MssqlConfig
from metaphor.mssql.model import MssqlDatabase, MssqlTable
from metaphor.mssql.mssql_client import MssqlClient

logger = get_logger()


class MssqlExtractor(BaseExtractor):
    """Mssql metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "MssqlExtractor":
        return MssqlExtractor(MssqlConfig.from_yaml_file(config_file))

    def __init__(
        self,
        config: MssqlConfig,
        description="Mssql metadata crawler",
        platform=Platform.MSSQL,
        dataset_platform=DataPlatform.MSSQL,
    ):
        super().__init__(config, description, platform)
        self._config = config
        self._config.server_name = self._config.server_name or self._config.endpoint
        self._filter = config.filter.normalize()
        self._platform = platform
        self._dataset_platform = dataset_platform

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from Mssql server: {self._config.server_name}")

        client = MssqlClient(
            self._config.endpoint, self._config.username, self._config.password
        )

        entities: List[ENTITY_TYPES] = []
        try:
            for database in client.get_databases():
                if not self._filter.include_database(database.name):
                    continue
                tables = client.get_tables(database.name)
                datasets = self._map_tables_to_dataset(
                    self._config.server_name, database, tables
                )
                self._set_foreign_keys_to_dataset(datasets, database.name, client)
                entities.extend(datasets.values())
        except Exception as error:
            logger.exception(f"mssql extract error: {error}")
        return entities

    def _map_tables_to_dataset(
        self,
        sql_server_name: str,
        database: MssqlDatabase,
        tables: List[MssqlTable],
    ):
        dataset_map: Dict[str, Dataset] = {}
        if len(tables) == 0:
            return dataset_map

        for table in tables:
            if not self._filter.include_table(
                database.name, table.schema_name, table.name
            ):
                continue
            dataset = Dataset()
            dataset.created_at = table.create_time
            dataset.logical_id = DatasetLogicalID(
                account=sql_server_name,
                name=dataset_normalized_name(
                    db=database.name, schema=table.schema_name, table=table.name
                ),
                platform=self._dataset_platform,
            )
            dataset.structure = DatasetStructure(
                database=database.name,
                schema=table.schema_name,
                table=table.name,
            )
            fields = []
            primary_keys = []
            for column in table.column_dict.values():
                fields.append(
                    SchemaField(
                        subfields=None,
                        field_name=column.name,
                        field_path=column.name,
                        max_length=column.max_length if column.max_length > 0 else None,
                        nullable=column.is_nullable,
                        precision=column.precision,
                        native_type=column.type,
                        is_unique=column.is_unique,
                    )
                )
                if column.is_primary_key:
                    primary_keys.append(column.name)

            dataset.custom_metadata = CustomMetadata(metadata=self._get_metadata(table))

            dataset.schema = DatasetSchema(
                sql_schema=SQLSchema(
                    table_schema=table.schema_name,
                    primary_key=primary_keys,
                    foreign_key=[],
                ),
                fields=fields,
            )

            if table.is_external:
                dataset.schema.sql_schema.materialization = MaterializationType.EXTERNAL
            elif table.type == "V":
                dataset.schema.sql_schema.materialization = MaterializationType.VIEW
            else:
                dataset.schema.sql_schema.materialization = MaterializationType.TABLE

            dataset_map[table.id] = dataset

        return dataset_map

    def _set_foreign_keys_to_dataset(
        self, dataset_map: Dict[str, Dataset], database_name: str, client: MssqlClient
    ):
        if len(dataset_map) == 0:
            return

        for fk in client.get_foreign_keys(database_name):
            if fk.table_id in dataset_map:
                foreign_key = ForeignKey(
                    field_path=fk.column_name,
                    parent=dataset_map[fk.referenced_table_id].logical_id
                    if fk.referenced_table_id in dataset_map
                    else None,
                    parent_field=fk.referenced_column,
                )
                dataset_map[fk.table_id].schema.sql_schema.foreign_key.append(
                    foreign_key
                )

    def _get_metadata(self, table: MssqlTable) -> List[CustomMetadataItem]:
        items: List[CustomMetadataItem] = []

        if self._config.tenant_id:
            items.append(
                CustomMetadataItem("tenant_id", json.dumps(self._config.tenant_id))
            )

        if table.external_file_format:
            items.append(
                CustomMetadataItem("format", json.dumps(table.external_file_format))
            )

        if table.external_source:
            items.append(
                CustomMetadataItem("source", json.dumps(table.external_source))
            )
        return items
