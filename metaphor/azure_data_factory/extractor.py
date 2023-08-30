from dataclasses import dataclass
from typing import Any, Collection, Dict, List, Optional
from urllib.parse import ParseResult, parse_qs, urlparse

import azure.mgmt.datafactory.models as DfModels
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from metaphor.azure_data_factory.config import AzureDataFactoryRunConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetStructure,
    DatasetUpstream,
)

logger = get_logger()


@dataclass
class Factory:
    name: str
    resource_group_name: str


@dataclass
class LinkedService:
    database: Optional[str] = None
    account: Optional[str] = None
    project: Optional[str] = None


class AzureDataFactoryExtractor(BaseExtractor):
    """Azure Data Factory metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "AzureDataFactoryExtractor":
        return AzureDataFactoryExtractor(
            AzureDataFactoryRunConfig.from_yaml_file(config_file)
        )

    def __init__(
        self,
        config: AzureDataFactoryRunConfig,
    ):
        super().__init__(config, "Azure Data Factory metadata crawler", Platform.GLUE)
        self._config = config

        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        df_client = self._build_client(self._config)

        for factory in self._get_factories(df_client):
            self.extract_for_factory(factory, df_client)

        return self._datasets.values()

    def extract_for_factory(
        self, factory: Factory, df_client: DataFactoryManagementClient
    ) -> None:
        logger.info(f"Fetching metadata from Azure Data Factory: {factory.name}")

        factory_datasets = self._get_datasets(factory, df_client)

        # Get table lineage from data flow
        self._extract_table_lineage(factory, df_client, factory_datasets)

    @staticmethod
    def _build_client(config: AzureDataFactoryRunConfig) -> DataFactoryManagementClient:
        return DataFactoryManagementClient(
            credential=ClientSecretCredential(
                client_id=config.client_id,
                client_secret=config.client_secret,
                tenant_id=config.tenant_id,
            ),
            subscription_id=config.subscription_id,
        )

    @staticmethod
    def _get_resource_group_from_factory(factory: DfModels.Factory):
        # id = /subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}
        return factory.id.split("/")[4]  # type: ignore

    def _get_factories(self, client: DataFactoryManagementClient) -> List[Factory]:
        return [
            Factory(
                name=factory.name,  # type: ignore
                resource_group_name=self._get_resource_group_from_factory(factory),
            )
            for factory in client.factories.list()
        ]

    def _extract_table_lineage(
        self,
        factory: Factory,
        client: DataFactoryManagementClient,
        factory_datasets: Dict[str, DfModels.Dataset],
    ) -> None:
        factory_data_flows = []

        for dataflow in client.data_flows.list_by_factory(
            factory_name=factory.name,
            resource_group_name=factory.resource_group_name,
        ):
            if isinstance(dataflow.properties, DfModels.MappingDataFlow):
                mappingDataflow: DfModels.MappingDataFlow = dataflow.properties

                def get_dataset_from_source_or_sink(
                    transformation: DfModels.Transformation,
                ) -> Optional[Dataset]:
                    if transformation.dataset is None:
                        return None

                    dataset_name = transformation.dataset.reference_name
                    dataset = factory_datasets.get(dataset_name)
                    if dataset is None:
                        logger.warning(f"Can not find dataset, name: {dataset_name}")
                        return None
                    return dataset

                sources = (
                    [
                        dataset
                        for dataset in map(
                            get_dataset_from_source_or_sink, mappingDataflow.sources
                        )
                        if dataset
                    ]
                    if mappingDataflow.sources
                    else []
                )

                sinks = (
                    [
                        dataset
                        for dataset in map(
                            get_dataset_from_source_or_sink, mappingDataflow.sinks
                        )
                        if dataset
                    ]
                    if mappingDataflow.sinks
                    else []
                )

                for source, sink in zip(sources, sinks):
                    sink.upstream.source_datasets.append(
                        str(to_dataset_entity_id_from_logical_id(source.logical_id))
                    )

                # Dump data flow for debug purpose
                factory_data_flows.append(dataflow.as_dict())

            json_dump_to_debug_file(
                factory_data_flows, f"{factory.name}_data_flows.json"
            )

    def _get_datasets(
        self, factory: Factory, client: DataFactoryManagementClient
    ) -> Dict[str, DfModels.Dataset]:
        linked_services = self._get_linked_services(factory, client)

        datasets = client.datasets.list_by_factory(
            factory_name=factory.name,
            resource_group_name=factory.resource_group_name,
        )

        result: Dict[str, DfModels.Dataset] = {}
        factory_datasets: List[Any] = []
        for dataset in datasets:
            linked_service_name = dataset.properties.linked_service_name.reference_name
            linked_service = linked_services.get(linked_service_name)
            if linked_service is None:
                logger.warning(
                    f"Can not found the linked service for dataset: {dataset.name}, linked_service_name: {linked_service_name}"
                )
                continue

            if isinstance(dataset.properties, DfModels.SnowflakeDataset):
                snowflake_dataset: DfModels.SnowflakeDataset = dataset.properties

                def get_schema(
                    snowflake_dataset: DfModels.SnowflakeDataset,
                ) -> Optional[str]:
                    schema = snowflake_dataset.schema_type_properties_schema
                    if isinstance(schema, str):
                        return schema
                    else:
                        logger.warning(f"Unable to get schema name from {schema}")
                        return None

                def get_table(
                    snowflake_dataset: DfModels.SnowflakeDataset,
                ) -> Optional[str]:
                    table = snowflake_dataset.table
                    if isinstance(table, str):
                        return table
                    else:
                        logger.warning(f"Unable to get table name from {table}")
                        return None

                schema = get_schema(snowflake_dataset)
                table = get_table(snowflake_dataset)
                database = linked_service.database

                metaphor_dataset = Dataset(
                    logical_id=DatasetLogicalID(
                        account=linked_service.account,
                        name=dataset_normalized_name(database, schema, table),
                        platform=DataPlatform.SNOWFLAKE,
                    ),
                    upstream=DatasetUpstream(source_datasets=[]),
                    structure=DatasetStructure(
                        database=database, schema=schema, table=table
                    ),
                )

                dataset_name: str = dataset.name  # type: ignore
                result[dataset_name] = metaphor_dataset
                dataset_id: str = dataset.id  # type: ignore
                self._datasets[dataset_id] = metaphor_dataset

            # Dump dataset for debug-purpose
            factory_datasets.append(dataset.as_dict())

        json_dump_to_debug_file(factory_datasets, f"{factory.name}_datasets.json")

        return result

    def _get_linked_services(
        self, factory: Factory, client: DataFactoryManagementClient
    ) -> Dict[str, LinkedService]:
        linked_services = client.linked_services.list_by_factory(
            factory_name=factory.name,
            resource_group_name=factory.resource_group_name,
        )

        result: Dict[str, LinkedService] = {}
        factory_linked_services = []
        for linked_service in linked_services:
            if isinstance(linked_service.properties, DfModels.SnowflakeLinkedService):
                snowflake_connection: DfModels.SnowflakeLinkedService = (
                    linked_service.properties
                )
                connection_string = snowflake_connection.connection_string

                if not isinstance(connection_string, str):
                    logger.warning(
                        f"unknown connection string for {linked_service.name}, connection string: {connection_string}"
                    )
                    continue

                url: ParseResult = urlparse(connection_string)
                query_db = parse_qs(url.query or "").get("db")
                database = query_db[0] if query_db else None

                # extract snowflake account name from jdbc format, 'snowflake://<account>.snowflakecomputing.com/'
                hostname = urlparse(url.path).hostname
                snowflake_account = (
                    ".".join(hostname.split(".")[:-2]) if hostname else None
                )

                result[linked_service.name] = LinkedService(
                    database=database, account=snowflake_account
                )

            # Dump linked service for debug-purpose
            factory_linked_services.append(linked_service.as_dict())

        json_dump_to_debug_file(
            factory_linked_services, f"{factory.name}_linked_services.json"
        )

        return result
