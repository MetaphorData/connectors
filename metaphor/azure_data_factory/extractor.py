import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import IO, Any, Collection, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import azure.mgmt.datafactory.models as DfModels
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from metaphor.azure_data_factory.config import AzureDataFactoryRunConfig
from metaphor.azure_data_factory.utils import (
    LinkedService,
    init_azure_sql_table_dataset,
    init_delimited_text_dataset,
    init_json_dataset,
    init_parquet_dataset,
    init_rest_dataset,
    init_snowflake_dataset,
    process_azure_sql_linked_service,
    process_snowflake_linked_service,
    safe_get_from_json,
)
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    to_dataset_entity_id_from_logical_id,
    to_pipeline_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.utils import safe_float, unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    ActivityDependency,
    AzureDataFactoryActivity,
    AzureDataFactoryPipeline,
    Dataset,
    DependencyCondition,
    Pipeline,
    PipelineInfo,
    PipelineLogicalID,
    PipelineMapping,
    PipelineType,
)

logger = get_logger()


@dataclass
class Factory:
    name: str
    id: str
    resource_group_name: str


class AzureDataFactoryExtractor(BaseExtractor):
    """Azure Data Factory metadata extractor"""

    _description = "Azure Data Factory metadata crawler"
    _platform = Platform.AZURE_DATA_FACTORY_PIPELINE

    @staticmethod
    def from_config_file(config_file: str) -> "AzureDataFactoryExtractor":
        return AzureDataFactoryExtractor(
            AzureDataFactoryRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: AzureDataFactoryRunConfig):
        super().__init__(config)
        self._config = config

        self._datasets: Dict[str, Dataset] = {}
        self._pipelines: Dict[str, Pipeline] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        df_client = self._build_client(self._config)

        for factory in self._get_factories(df_client):
            self.extract_for_factory(factory, df_client)

        # Remove duplicate source_entity and source_dataset, and set None for empty upstream data
        for dataset in self._datasets.values():
            unique_source_entities = unique_list(
                dataset.entity_upstream.source_entities
            )
            if len(unique_source_entities) == 0:
                dataset.entity_upstream = None
            else:
                dataset.entity_upstream.source_entities = unique_source_entities

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
                sink.entity_upstream.source_entities.append(source_entity_id)

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

        for dataset_resource in datasets:
            dataset = dataset_resource.properties

            # Dump dataset for debug-purpose
            factory_datasets_list.append(dataset_resource.as_dict())

            linked_service_name = dataset.linked_service_name.reference_name
            linked_service = linked_services.get(linked_service_name)
            if linked_service is None:
                logger.warning(
                    f"Can not found the linked service for dataset: {dataset_resource.name}, linked_service_name: {linked_service_name}"
                )
                continue

            dataset_name: str = dataset_resource.name  # type: ignore
            dataset_id: str = dataset_resource.id  # type: ignore
            metaphor_dataset: Optional[Dataset] = None

            if isinstance(dataset, DfModels.SnowflakeDataset):
                metaphor_dataset = init_snowflake_dataset(dataset, linked_service)

            if isinstance(dataset, DfModels.AzureSqlTableDataset):
                metaphor_dataset = init_azure_sql_table_dataset(dataset, linked_service)

            if isinstance(dataset, DfModels.JsonDataset):
                metaphor_dataset = init_json_dataset(dataset, linked_service)

            if isinstance(dataset, DfModels.RestResourceDataset):
                metaphor_dataset = init_rest_dataset(dataset, linked_service)

            if isinstance(dataset, DfModels.ParquetDataset):
                metaphor_dataset = init_parquet_dataset(dataset, linked_service)

            if isinstance(dataset, DfModels.DelimitedTextDataset):
                metaphor_dataset = init_delimited_text_dataset(dataset, linked_service)

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
        for linked_service_resource in linked_services:
            linked_service_name: str = linked_service_resource.name  # type: ignore
            linked_service = linked_service_resource.properties

            if isinstance(linked_service, DfModels.SnowflakeLinkedService):
                snowflake_linked_service = process_snowflake_linked_service(
                    linked_service, linked_service_name
                )
                if snowflake_linked_service:
                    result[linked_service_name] = snowflake_linked_service
            if isinstance(
                linked_service,
                DfModels.AzureSqlDatabaseLinkedService,
            ):
                azure_sql_linked_service = process_azure_sql_linked_service(
                    linked_service, linked_service_name
                )
                if azure_sql_linked_service:
                    result[linked_service_name] = azure_sql_linked_service

            if isinstance(
                linked_service,
                DfModels.AzureBlobStorageLinkedService,
            ):
                service_endpoint: Optional[str] = linked_service.service_endpoint  # type: ignore
                result[linked_service_name] = LinkedService(account=service_endpoint)

            if isinstance(linked_service, DfModels.AzureBlobFSLinkedService):
                url = safe_get_from_json(linked_service.url)
                result[linked_service_name] = LinkedService(url=url)

            if isinstance(linked_service, DfModels.HttpLinkedService):
                url = safe_get_from_json(linked_service.url)

                result[linked_service_name] = LinkedService(url=url)

            if isinstance(linked_service, DfModels.RestServiceLinkedService):
                url = safe_get_from_json(linked_service.url)
                result[linked_service_name] = LinkedService(url=url)

            # Dump linked service for debug-purpose
            factory_linked_services.append(linked_service_resource.as_dict())

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
        metaphor_activities, pipeline_sources, pipeline_sinks = [], [], []

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
                        pipeline_sources.append(dataset_id)
                        copy_from.append(dataset_id)

                for dataset_reference in activity.outputs or []:
                    dataset = factory_datasets.get(dataset_reference.reference_name)
                    if dataset:
                        pipeline_sinks.append(
                            str(
                                to_dataset_entity_id_from_logical_id(dataset.logical_id)
                            )
                        )

                        # Copy activity table lineage
                        for source_id in copy_from:
                            dataset.entity_upstream.source_entities.append(source_id)

                        # Pipeline info for each copy activity output entity
                        dataset.pipeline_info = PipelineInfo(
                            pipeline_mapping=[
                                PipelineMapping(
                                    is_virtual=False,
                                    source_entity_id=source_id,
                                    pipeline_entity_id=pipeline_entity_id,
                                )
                                for source_id in copy_from
                            ]
                        )

            if isinstance(activity, DfModels.ExecuteDataFlowActivity):
                data_flow = factory_data_flows.get(activity.data_flow.reference_name)
                (
                    data_flow_sources,
                    data_flow_sinks,
                ) = AzureDataFactoryExtractor._get_sinks_and_source_from_data_flow(
                    data_flow, factory_datasets
                )

                activity_sources = [
                    str(to_dataset_entity_id_from_logical_id(source_dataset.logical_id))
                    for source_dataset in data_flow_sources
                ]

                pipeline_sources.extend(activity_sources)

                for sink_dataset in data_flow_sinks:
                    pipeline_sinks.append(
                        str(
                            to_dataset_entity_id_from_logical_id(
                                sink_dataset.logical_id
                            )
                        )
                    )

                    # Pipeline info for each sink of a dataflow activity
                    sink_dataset.pipeline_info = PipelineInfo(
                        pipeline_mapping=[
                            PipelineMapping(
                                is_virtual=False,
                                source_entity_id=source_id,
                                pipeline_entity_id=pipeline_entity_id,
                            )
                            for source_id in activity_sources
                        ]
                    )

        return metaphor_activities, pipeline_sources, pipeline_sinks

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

        factory_pipelines = []

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
                last_duration_in_ms = safe_float(last_pipeline_run.duration_in_ms)  # type: ignore
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
            factory_pipelines.append(pipeline.as_dict())

        json_dump_to_debug_file(factory_pipelines, f"{factory.name}_pipelines.json")
