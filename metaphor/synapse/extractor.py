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
    MaterializationType,
    SchemaField,
    SQLSchema,
)
from metaphor.synapse.auth_client import AuthClient
from metaphor.synapse.config import SynapseConfig
from metaphor.synapse.workspace_client import (
    DedicatedSqlPoolTable,
    SynapseTable,
    SynapseWorkspace,
    WorkspaceDatabase,
)

logger = get_logger()


class SynapseExtractor(BaseExtractor):
    """Synapse metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SynapseExtractor":
        return SynapseExtractor(SynapseConfig.from_yaml_file(config_file))

    def __init__(self, config: SynapseConfig):
        super().__init__(config, "Synapse metadata crawler", Platform.SYNAPSE)
        self._client = AuthClient(config)
        self._databases: List[WorkspaceDatabase] = []
        self._dataset_map: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        for workspaceClient in self._client.get_list_workspace_clients():
            logger.info(
                f"Fetching metadata from Synapse workspace ID: {workspaceClient._workspace.id}"
            )
            # serverless sqlpool
            try:
                for database in workspaceClient.get_databases():
                    tables = workspaceClient.get_tables(database.name)
                    self._map_tables_to_dataset(
                        workspaceClient._workspace, database, tables
                    )

                # workspaceClient.get_dedicated_sql_pool_query_log()
            except Exception as error:
                logger.exception(error)

            # dedicated sqlpool
            try:
                for database in workspaceClient.get_dedicated_sql_pool_databases():
                    tables = workspaceClient.get_dedicated_sql_pool_tables(
                        database.name
                    )
                    self._map_dedicated_sql_pool_tables_to_dataset(
                        workspaceClient._workspace, database, tables
                    )

                    # workspaceClient.get_dedicated_sql_pool_query_log(database.name, True)
            except Exception as error:
                logger.exception(error)

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._dataset_map.values())
        return entities

    def _map_tables_to_dataset(
        self,
        workspace: SynapseWorkspace,
        database: WorkspaceDatabase,
        tables: List[SynapseTable],
    ):
        for table in tables:
            dataset = Dataset()
            dataset.entity_type = EntityType.DATASET
            dataset.logical_id = DatasetLogicalID(
                account=workspace.name,
                name=dataset_normalized_name(db=database.name, table=table.name),
                platform=DataPlatform.SYNAPSE,
            )
            dataset.structure = DatasetStructure(
                database=database.name,
                table=table.name,
            )
            fields = []
            for column in table.properties["StorageDescriptor"]["Columns"]:
                fields.append(
                    SchemaField(
                        subfields=None,
                        field_name=column["Name"],
                        field_path=column["Name"],
                        description=column["Description"],
                        max_length=float(column["OriginDataTypeName"]["Length"]),
                        nullable=column["OriginDataTypeName"]["IsNullable"],
                        precision=float(column["OriginDataTypeName"]["Precision"]),
                        native_type=column["OriginDataTypeName"]["TypeName"],
                    )
                )

            dataset.custom_metadata = CustomMetadata(
                metadata=self._get_metadata(table.properties["StorageDescriptor"])
            )

            dataset.schema = DatasetSchema(
                sql_schema=SQLSchema(
                    table_schema=table.properties["TableType"],
                    primary_key=self._get_primary_keys(table.properties["Properties"]),
                ),
                fields=fields,
            )

            if table.properties["TableType"] == "EXTERNAL":
                dataset.schema.sql_schema.materialization = MaterializationType.EXTERNAL
            elif table.properties["TableType"] == "VIEW":
                dataset.schema.sql_schema.materialization = MaterializationType.VIEW
            else:
                dataset.schema.sql_schema.materialization = MaterializationType.TABLE

            self._dataset_map[table.id] = dataset

    def _get_primary_keys(self, properties: Dict[str, str]) -> List[str]:
        primary_keys = []
        if "PrimaryKeys" in properties:
            primary_keys = properties["PrimaryKeys"].split(",")
        return primary_keys

    def _get_metadata(
        self, meta_data: Dict[str, Dict] = None
    ) -> List[CustomMetadataItem]:
        items: List[CustomMetadataItem] = []

        items.append(
            CustomMetadataItem("tenant_id", json.dumps(self._client._tenant_id))
        )

        if meta_data:
            if "Format" in meta_data and "FormatType" in meta_data["Format"]:
                items.append(
                    CustomMetadataItem(
                        "format", json.dumps(meta_data["Format"]["FormatType"])
                    )
                )

            if "Source" in meta_data and "Location" in meta_data["Source"]:
                items.append(
                    CustomMetadataItem(
                        "source", json.dumps(meta_data["Source"]["Location"])
                    )
                )
        return items

    def _map_dedicated_sql_pool_tables_to_dataset(
        self,
        workspace: SynapseWorkspace,
        database: WorkspaceDatabase,
        tables: List[DedicatedSqlPoolTable],
    ):
        for table in tables:
            dataset = Dataset()
            dataset.logical_id = DatasetLogicalID(
                account=workspace.name,
                name=dataset_normalized_name(
                    db=database.name,
                    schema=table.sqlSchema.name,
                    table=table.name,
                ),
                platform=DataPlatform.SYNAPSE,
            )
            dataset.structure = DatasetStructure(
                database=database.name,
                schema=table.sqlSchema.name,
                table=table.name,
            )
            fields = []
            for column in table.columns:
                fields.append(
                    SchemaField(
                        subfields=None,
                        field_name=column["name"],
                        field_path=column["name"],
                        native_type=column["properties"]["columnType"],
                    )
                )

            dataset.custom_metadata = CustomMetadata(metadata=self._get_metadata())

            dataset.schema = DatasetSchema(
                sql_schema=SQLSchema(table_schema=table.sqlSchema.name),
                fields=fields,
            )

            if table.sqlSchema.name == "view":
                dataset.schema.sql_schema.materialization = MaterializationType.VIEW
            else:
                dataset.schema.sql_schema.materialization = MaterializationType.TABLE

            self._dataset_map[table.id] = dataset
