import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import IO, Any, Collection, Dict, List, Optional, Tuple
from urllib.parse import ParseResult, parse_qs, urlencode, urljoin, urlparse

import azure.mgmt.datafactory.models as DfModels
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from metaphor.azure_data_factory.config import AzureDataFactoryRunConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    to_dataset_entity_id_from_logical_id,
    to_pipeline_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.utils import removesuffix, unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    ActivityDependency,
    AzureDataFactoryActivity,
    AzureDataFactoryPipeline,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUpstream,
    DependencyCondition,
    Pipeline,
    PipelineLogicalID,
    PipelineType,
)

logger = get_logger()


@dataclass
class Factory:
    name: str
    id: str
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
        self._pipelines: Dict[str, Pipeline] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        df_client = self._build_client(self._config)

        for factory in self._get_factories(df_client):
            self.extract_for_factory(factory, df_client)

        # Remove duplicate source_dataset, and set None for empty upstream data
        for dataset in self._datasets.values():
            unique_source_dataset = unique_list(dataset.upstream.source_datasets)
            if len(unique_source_dataset) == 0:
                dataset.upstream = None
            else:
                dataset.upstream.source_datasets = unique_source_dataset

        return list(chain(self._datasets.values(), self._pipelines.values()))

    def extract_for_factory(
        self, factory: Factory, df_client: DataFactoryManagementClient
    ) -> None:
        logger.info(f"Fetching metadata from Azure Data Factory: {factory.name}")

        factory_datasets = self._get_datasets(factory, df_client)
        factory_data_flows = self._get_data_flows(factory, df_client)

        # Get table lineage from data flow
        self._extract_table_lineage(factory_data_flows, factory_datasets)

        self._extract_pipeline_metadata(
            factory, df_client, factory_datasets, factory_data_flows
        )

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
                id=factory.id,  # type: ignore
                resource_group_name=self._get_resource_group_from_factory(factory),
            )
            for factory in client.factories.list()
        ]

    @staticmethod
    def _get_sinks_and_source_from_data_flow(
        data_flow: Optional[DfModels.DataFlowResource],
        factory_datasets: Dict[str, Dataset],
    ) -> Tuple[List[Dataset], List[Dataset]]:
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

        sources, sinks = [], []
        if data_flow and isinstance(data_flow.properties, DfModels.MappingDataFlow):
            mapping_data_flow = data_flow.properties
            sources = (
                [
                    dataset
                    for dataset in map(
                        get_dataset_from_source_or_sink, mapping_data_flow.sources
                    )
                    if dataset
                ]
                if mapping_data_flow.sources
                else []
            )

            sinks = (
                [
                    dataset
                    for dataset in map(
                        get_dataset_from_source_or_sink, mapping_data_flow.sinks
                    )
                    if dataset
                ]
                if mapping_data_flow.sinks
                else []
            )
        return sources, sinks

    def _extract_table_lineage(
        self,
        factory_data_flows: Dict[str, DfModels.DataFlowResource],
        factory_datasets: Dict[str, Dataset],
    ) -> None:
        for data_flow in factory_data_flows.values():
            sources, sinks = self._get_sinks_and_source_from_data_flow(
                data_flow, factory_datasets
            )
            for source, sink in zip(sources, sinks):
                source_entity_id = str(
                    to_dataset_entity_id_from_logical_id(source.logical_id)
                )
                sink.upstream.source_datasets.append(source_entity_id)

    def _get_data_flows(
        self, factory: Factory, client: DataFactoryManagementClient
    ) -> Dict[str, DfModels.DataFlowResource]:
        factory_data_flows: Dict[str, DfModels.DataFlowResource] = {}
        factory_data_flows_list = []

        for data_flow in client.data_flows.list_by_factory(
            factory_name=factory.name,
            resource_group_name=factory.resource_group_name,
        ):
            data_flow_name: str = data_flow.name  # type: ignore
            factory_data_flows[data_flow_name] = data_flow

            # Dump data flow for debug purpose
            factory_data_flows_list.append(data_flow.as_dict())

        json_dump_to_debug_file(
            factory_data_flows_list, f"{factory.name}_data_flows.json"
        )

        return factory_data_flows

    @staticmethod
    def _safe_get_from_json(prop: Any) -> Optional[str]:
        if isinstance(prop, str):
            return prop
        else:
            logger.warning(f"Unable to get value from {prop}")
            return None

    def _get_datasets(
        self, factory: Factory, client: DataFactoryManagementClient
    ) -> Dict[str, DfModels.Dataset]:
        linked_services = self._get_linked_services(factory, client)

        datasets = client.datasets.list_by_factory(
            factory_name=factory.name,
            resource_group_name=factory.resource_group_name,
        )

        factory_datasets: Dict[str, Dataset] = {}

        # Capture all dataset for debug purpose
        factory_datasets_list: List[Any] = []

        for dataset in datasets:
            # Dump dataset for debug-purpose
            factory_datasets_list.append(dataset.as_dict())

            linked_service_name = dataset.properties.linked_service_name.reference_name
            linked_service = linked_services.get(linked_service_name)
            if linked_service is None:
                logger.warning(
                    f"Can not found the linked service for dataset: {dataset.name}, linked_service_name: {linked_service_name}"
                )
                continue

            dataset_name: str = dataset.name  # type: ignore
            dataset_id: str = dataset.id  # type: ignore
            metaphor_dataset: Optional[Dataset] = None

            if isinstance(dataset.properties, DfModels.SnowflakeDataset):
                snowflake_dataset: DfModels.SnowflakeDataset = dataset.properties

                schema = self._safe_get_from_json(
                    snowflake_dataset.schema_type_properties_schema
                )
                table = self._safe_get_from_json(snowflake_dataset.table)
                database = linked_service.database

                metaphor_dataset = Dataset(
                    logical_id=DatasetLogicalID(
                        account=linked_service.account,
                        name=dataset_normalized_name(database, schema, table),
                        platform=DataPlatform.SNOWFLAKE,
                    ),
                    upstream=DatasetUpstream(source_datasets=[]),
                )

            if isinstance(dataset.properties, DfModels.AzureSqlTableDataset):
                sql_table_dataset = dataset.properties

                schema = self._safe_get_from_json(
                    sql_table_dataset.schema_type_properties_schema
                )
                table = self._safe_get_from_json(sql_table_dataset.table)

                metaphor_dataset = Dataset(
                    logical_id=DatasetLogicalID(
                        account=linked_service.account,
                        name=dataset_normalized_name(
                            linked_service.database, schema, table
                        ),
                        platform=DataPlatform.MSSQL,
                    ),
                    upstream=DatasetUpstream(source_datasets=[]),
                )

            if isinstance(dataset.properties, DfModels.JsonDataset):
                json_dataset = dataset.properties

                if (
                    isinstance(json_dataset.location, DfModels.AzureBlobStorageLocation)
                    and linked_service.account
                ):
                    storage_account = linked_service.account
                    abs_location = json_dataset.location

                    parts: List[str] = [
                        part  # type: ignore
                        for part in [
                            abs_location.container,
                            abs_location.folder_path,
                            abs_location.file_name,
                        ]
                        if part is not None and isinstance(part, str)
                    ]

                    full_path = urljoin(storage_account, "/".join(parts))

                    metaphor_dataset = Dataset(
                        logical_id=DatasetLogicalID(
                            name=full_path,
                            platform=DataPlatform.AZURE_BLOB_STORAGE,
                        ),
                        upstream=DatasetUpstream(source_datasets=[]),
                    )

            if metaphor_dataset:
                factory_datasets[dataset_name] = metaphor_dataset

                # Store to global dataset dict
                self._datasets[dataset_id] = metaphor_dataset

        json_dump_to_debug_file(factory_datasets_list, f"{factory.name}_datasets.json")

        return factory_datasets

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
            linked_service_name: str = linked_service.name  # type: ignore

            if isinstance(linked_service.properties, DfModels.SnowflakeLinkedService):
                snowflake_connection: DfModels.SnowflakeLinkedService = (
                    linked_service.properties
                )
                connection_string = self._safe_get_from_json(
                    snowflake_connection.connection_string
                )

                if connection_string is None:
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
                    removesuffix(hostname, ".snowflakecomputing.com")
                    if hostname
                    else None
                )

                result[linked_service_name] = LinkedService(
                    database=database, account=snowflake_account
                )
            if isinstance(
                linked_service.properties, DfModels.AzureSqlDatabaseLinkedService
            ):
                sql_database = linked_service.properties

                # format: <key>=<value>;<key>=<value>
                connection_string = self._safe_get_from_json(
                    sql_database.connection_string
                )

                if connection_string is None:
                    logger.warning(
                        f"unknown connection string for {linked_service.name}, connection string: {connection_string}"
                    )
                    continue

                server_host, database = None, None
                for kv_pair in connection_string.split(";"):
                    [key, value] = kv_pair.split("=") if "=" else ["", ""]
                    if key == "Data Source":
                        server_host = (
                            removesuffix(str(value), ".database.windows.net")
                            if value
                            else None
                        )
                    if key == "Initial Catalog":
                        database = value

                result[linked_service_name] = LinkedService(
                    database=database, account=server_host
                )

            if isinstance(
                linked_service.properties, DfModels.AzureBlobStorageLinkedService
            ):
                blob_storage = linked_service.properties
                service_endpoint = blob_storage.service_endpoint

                result[linked_service_name] = LinkedService(account=service_endpoint)

            # Dump linked service for debug-purpose
            factory_linked_services.append(linked_service.as_dict())

        json_dump_to_debug_file(
            factory_linked_services, f"{factory.name}_linked_services.json"
        )

        return result

    @staticmethod
    def _get_last_pipeline_run(
        pipeline_name: str,
        df_client: DataFactoryManagementClient,
        factory: Factory,
    ) -> Optional[DfModels.PipelineRun]:
        current_time = datetime.now(tz=timezone.utc)

        filter_parameters = DfModels.RunFilterParameters(
            last_updated_before=current_time,
            last_updated_after=current_time - timedelta(days=7),
            filters=[
                DfModels.RunQueryFilter(
                    operand=DfModels.RunQueryFilterOperand.PIPELINE_NAME,
                    operator=DfModels.RunQueryFilterOperator.EQUALS,
                    values=[pipeline_name],
                ),
                # We should add this filter here but the spec of filters.values is [str], python would convert bool into str first, so we need do hack this around ourselves.
                # ---
                # DfModels.RunQueryFilter(
                #     operand=DfModels.RunQueryFilterOperand.LATEST_ONLY,
                #     operator=DfModels.RunQueryFilterOperator.EQUALS,
                #     values=[True],
                # ),
            ],
        )

        # Patch filter body
        body = df_client.pipeline_runs._serialize.body(
            filter_parameters, "RunFilterParameters"
        )
        body["filters"].append(
            {
                "operand": "LatestOnly",
                "operator": "Equals",
                "values": [True],
            }
        )

        # bytes also works, cast to IO[bytes] to make mypy happy
        payload: IO[bytes] = bytes(json.dumps(body), "ascii")  # type: ignore

        response: DfModels.PipelineRunsQueryResponse = (
            df_client.pipeline_runs.query_by_factory(
                resource_group_name=factory.resource_group_name,
                factory_name=factory.name,
                filter_parameters=payload,
            )
        )

        if isinstance(response.value, list) and len(response.value) > 0:
            return response.value[0]
        return None

    @staticmethod
    def _map_dependency_conditions(conditions: list) -> List[DependencyCondition]:
        result: List[DependencyCondition] = []
        for condition in conditions:
            if isinstance(condition, str):
                try:
                    result.append(DependencyCondition(condition))
                except ValueError:
                    logger.warning(
                        f"Invalid enum value for DependencyCondition: {condition}"
                    )
                    continue
        return result

    @staticmethod
    def _process_activities(
        pipeline_entity_id: str,
        activities: List[DfModels.Activity],
        factory_datasets: Dict[str, Dataset],
        factory_data_flows: Dict[str, DfModels.DataFlowResource],
    ) -> Tuple[List[AzureDataFactoryActivity], List[str], List[str]]:
        metaphor_activities, sources, sinks = [], [], []

        for activity in activities:
            metaphor_activities.append(
                AzureDataFactoryActivity(
                    depends_on=[
                        ActivityDependency(
                            dependency_conditions=AzureDataFactoryExtractor._map_dependency_conditions(
                                dependent.dependency_conditions
                            ),
                            name=dependent.activity,
                        )
                        for dependent in (activity.depends_on or [])
                    ],
                    name=activity.name,
                    type=activity.type,
                )
            )

            if isinstance(activity, DfModels.CopyActivity):
                copy_from: List[str] = []

                for dataset_reference in activity.inputs or []:
                    dataset = factory_datasets.get(dataset_reference.reference_name)
                    if dataset:
                        dataset_id = str(
                            to_dataset_entity_id_from_logical_id(dataset.logical_id)
                        )
                        sources.append(dataset_id)
                        copy_from.append(dataset_id)

                for dataset_reference in activity.outputs or []:
                    dataset = factory_datasets.get(dataset_reference.reference_name)
                    if dataset:
                        sinks.append(
                            str(
                                to_dataset_entity_id_from_logical_id(dataset.logical_id)
                            )
                        )

                        # Copy activity table lineage
                        for source_id in copy_from:
                            dataset.upstream.source_datasets.append(source_id)

                        # Assign pipeline entityId to output's upstream
                        dataset.upstream.pipeline_entity_id = pipeline_entity_id

            if isinstance(activity, DfModels.ExecuteDataFlowActivity):
                data_flow = factory_data_flows.get(activity.data_flow.reference_name)
                (
                    data_flow_sources,
                    data_flow_sinks,
                ) = AzureDataFactoryExtractor._get_sinks_and_source_from_data_flow(
                    data_flow, factory_datasets
                )

                for source_dataset in data_flow_sources:
                    sources.append(
                        str(
                            to_dataset_entity_id_from_logical_id(
                                source_dataset.logical_id
                            )
                        )
                    )

                for sink_dataset in data_flow_sinks:
                    sinks.append(
                        str(
                            to_dataset_entity_id_from_logical_id(
                                sink_dataset.logical_id
                            )
                        )
                    )

                    # Assign pipeline entityId to sink's upstream
                    sink_dataset.upstream.pipeline_entity_id = pipeline_entity_id

        return metaphor_activities, sources, sinks

    def _extract_pipeline_metadata(
        self,
        factory: Factory,
        df_client: DataFactoryManagementClient,
        factory_datasets: Dict[str, Dataset],
        factory_data_flows: Dict[str, DfModels.DataFlowResource],
    ) -> None:
        # The response from REST api was "typeProperties.dataflow" insteadOf "typeProperties.dataflow"
        # Patch the model in run time to get correct deserialized content
        df_client.pipelines.models.ExecuteDataFlowActivity._attribute_map[
            "data_flow"
        ] = {"key": "typeProperties.dataflow", "type": "DataFlowReference"}

        for pipeline in df_client.pipelines.list_by_factory(
            resource_group_name=factory.resource_group_name, factory_name=factory.name
        ):
            pipeline_name: str = pipeline.name  # type: ignore
            pipeline_id: str = pipeline.id  # type: ignore

            # TODO: properties.lastPublishTime is not covered in model
            # last_publish_time = None
            last_duration_in_ms: Optional[float] = None
            last_invoke_type: Optional[str] = None
            last_run_message: Optional[str] = None
            last_run_end: Optional[datetime] = None
            last_run_start: Optional[datetime] = None
            last_run_status: Optional[str] = None

            last_pipeline_run = self._get_last_pipeline_run(
                pipeline_name, df_client, factory
            )
            if last_pipeline_run:
                last_duration_in_ms = float(last_pipeline_run.duration_in_ms)  # type: ignore
                invoked_by: DfModels.PipelineRunInvokedBy = last_pipeline_run.invoked_by  # type: ignore
                last_invoke_type = invoked_by.invoked_by_type
                last_run_message = last_pipeline_run.message
                last_run_start = last_pipeline_run.run_start
                last_run_end = last_pipeline_run.run_end
                last_run_status = last_pipeline_run.status

            pipeline_logical_id = PipelineLogicalID(
                name=pipeline_id, type=PipelineType.AZURE_DATA_FACTORY_PIPELINE
            )

            activities, sources, sinks = self._process_activities(
                str(to_pipeline_entity_id_from_logical_id(pipeline_logical_id)),
                pipeline.activities or [],
                factory_datasets,
                factory_data_flows,
            )

            params = {"factory": factory.id}
            pipeline_url = f"https://adf.azure.com/authoring/pipeline/{pipeline_name}?{urlencode(params)}"

            metaphor_pipeline = Pipeline(
                logical_id=pipeline_logical_id,
                azure_data_factory_pipeline=AzureDataFactoryPipeline(
                    factory=factory.name,
                    pipeline_url=pipeline_url,
                    last_duration_in_ms=last_duration_in_ms,
                    last_invoke_type=last_invoke_type,
                    last_publish_time=None,
                    last_run_message=last_run_message,
                    last_run_start=last_run_start,
                    last_run_end=last_run_end,
                    last_run_status=last_run_status,
                    pipeline_name=pipeline_name,
                    activities=activities,
                    sinks=unique_list(sinks),
                    sources=unique_list(sources),
                ),
            )
            self._pipelines[pipeline_id] = metaphor_pipeline
