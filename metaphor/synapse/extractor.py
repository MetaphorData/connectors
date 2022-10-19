from typing import Collection, Dict, List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AspectType,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    EntityType,
    MaterializationType,
    SchemaField,
    SchemaType,
    SQLSchema,
)
from metaphor.synapse.auth_client import AuthClient
from metaphor.synapse.config import SynapseConfig
from metaphor.synapse.workspace_client import (
    SqlPoolTable,
    SynapseTable,
    WorkspaceDatabase,
)

logger = get_logger(__name__)


class SynapseExtractor(BaseExtractor):
    """Synapse metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SynapseExtractor":
        return SynapseExtractor(SynapseConfig.from_yaml_file(config_file))

    def __init__(self, config: SynapseConfig):
        super().__init__(config, "Synapse metadata crawler", Platform.SYNAPSE)
        self._tenant_id = config.tenant_id
        self._client = AuthClient(config)
        self._databases: List[WorkspaceDatabase] = []
        self._dataset_map: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        for workspaceClient in self._client.get_list_workspace_clients():
            logger.info(
                f"Fetching metadata from Synapse workspace ID: {workspaceClient._workspace.id}"
            )
            # database
            try:
                for database in workspaceClient.get_databases():
                    tables = workspaceClient.get_tables(database.name)
                    # views = self._client.get_view(workspace.name, database.name)
                    self._map_tables_to_dataset(database, tables)
            except Exception as e:
                logger.exception(e)

            # sqlpool database
            try:
                for database in workspaceClient.get_sqlpool_databases():
                    tables = workspaceClient.get_sqlpool_tables(database.name)
                    # views = self._client.get_view(workspace.name, database.name)
                    self._map_sqlpool_tables_to_dataset(database, tables)
            except Exception as e:
                logger.exception(e)

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._dataset_map.values())
        return entities

    def _map_tables_to_dataset(
        self, database: WorkspaceDatabase, tables: List[SynapseTable]
    ):
        for table in tables:
            dataset = Dataset()
            dataset.entity_type = EntityType.DATASET
            dataset.logical_id = DatasetLogicalID(
                name=table.id, platform=DataPlatform.SYNAPSE
            )
            dataset.display_name = table.name
            dataset.structure = DatasetStructure(
                id=database.id,
                aspect_type=AspectType.NAMESPACE_INFO,
                database=database.name,
                schema=database.properties["Origin"]["Type"],
                table=table.name,
            )
            fields = []
            for column in table.properties["StorageDescriptor"]["Columns"]:
                fields.append(
                    SchemaField(
                        subfields=None,
                        field_name=column["Name"],
                        description=column["Description"],
                        max_length=float(column["OriginDataTypeName"]["Length"]),
                        nullable=column["OriginDataTypeName"]["IsNullable"],
                        precision=float(column["OriginDataTypeName"]["Precision"]),
                        native_type=column["OriginDataTypeName"]["TypeName"],
                    )
                )

            dataset.schema = DatasetSchema(
                aspect_type=AspectType.DATASET_SCHEMA,
                schema_type=SchemaType.SQL,
                sql_schema=SQLSchema(table_schema=table.properties["TableType"]),
                fields=fields,
            )

            if table.properties["TableType"] == "EXTERNAL":
                dataset.schema.sql_schema.materialization = MaterializationType.EXTERNAL
            elif table.properties["TableType"] == "VIEW":
                dataset.schema.sql_schema.materialization = MaterializationType.VIEW
            else:
                dataset.schema.sql_schema.materialization = MaterializationType.TABLE

            self._dataset_map[table.id] = dataset

    def _map_sqlpool_tables_to_dataset(
        self, database: WorkspaceDatabase, tables: List[SqlPoolTable]
    ):
        for table in tables:
            dataset = Dataset()
            dataset.logical_id = DatasetLogicalID(
                name=table.id, platform=DataPlatform.SYNAPSE
            )
            dataset.display_name = table.name
            dataset.structure = DatasetStructure(
                id=database.id,
                aspect_type=AspectType.NAMESPACE_INFO,
                database=database.name,
                table=table.name,
            )
            fields = []
            for column in table.columns:
                fields.append(
                    SchemaField(
                        subfields=None,
                        field_name=column["name"],
                        native_type=column["properties"]["columnType"],
                    )
                )

            dataset.schema = DatasetSchema(
                id=table.sqlSchema.id,
                aspect_type=AspectType.DATASET_SCHEMA,
                sql_schema=SQLSchema(table_schema=table.sqlSchema.name),
                fields=fields,
            )

            if table.sqlSchema.name == "view":
                dataset.schema.sql_schema.materialization = MaterializationType.VIEW
            else:
                dataset.schema.sql_schema.materialization = MaterializationType.TABLE

            self._dataset_map[table.id] = dataset
