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
    EntityType,
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
        self.config = config
        self._platform = platform
        self._dataset_platform = dataset_platform

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from Mssql server: {self.config.server_name}")

        endpoint = f"{self.config.server_name}.database.windows.net"
        client = MssqlClient(endpoint, self.config.username, self.config.password)

        entities: List[ENTITY_TYPES] = []
        try:
            for database in client.get_databases():
                tables = client.get_tables(database.name)
                if len(tables) == 0:
                    continue
                datasets = self._map_tables_to_dataset(
                    self.config.server_name, database, tables, client
                )
                entities.extend(datasets)
        except Exception as error:
            logger.exception(f"serverless sqlpool error: {error}")
        return entities

    def _map_tables_to_dataset(
        self,
        sql_server_name: str,
        database: MssqlDatabase,
        tables: List[MssqlTable],
        client: MssqlClient,
    ):
        dataset_map: Dict[str, Dataset] = {}
        for table in tables:
            dataset = Dataset()
            dataset.created_at = table.create_time
            dataset.entity_type = EntityType.DATASET
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

        # get foreign keys
        for fk in client.get_foreign_keys(database.name):
            if fk.table_id in dataset_map:
                foreign_key = ForeignKey(
                    field_path=fk.column_name,
                    parent=dataset_map[fk.referenced_table_id].logical_id,
                    parent_field=fk.referenced_column,
                )
                dataset_map[fk.table_id].schema.sql_schema.foreign_key.append(
                    foreign_key
                )

        return dataset_map.values()

    def _get_metadata(self, table: MssqlTable) -> List[CustomMetadataItem]:
        items: List[CustomMetadataItem] = []

        items.append(CustomMetadataItem("tenant_id", json.dumps(self.config.tenant_id)))

        if table.external_file_format:
            items.append(
                CustomMetadataItem("format", json.dumps(table.external_file_format))
            )

        if table.external_source:
            items.append(
                CustomMetadataItem("source", json.dumps(table.external_source))
            )
        return items
