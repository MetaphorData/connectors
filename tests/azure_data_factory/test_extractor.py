from unittest.mock import MagicMock, patch

import azure.mgmt.datafactory.models as DfModels
import pytest

from metaphor.azure_data_factory.config import AzureDataFactoryRunConfig
from metaphor.azure_data_factory.extractor import AzureDataFactoryExtractor
from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from tests.test_utils import load_json


@patch("metaphor.azure_data_factory.extractor.AzureDataFactoryExtractor._build_client")
@pytest.mark.asyncio
async def test_extractor(mock_build_client: MagicMock, test_root_dir: str):
    mock_client = MagicMock()

    factory_name = "testADF"
    factory_id = (
        f"/subscriptions/<sub_id>/resourceGroups/<resource_group>/providers/Microsoft.DataFactory/factories/{factory_name}",
    )
    mock_client.factories.list.side_effect = lambda: [
        DfModels.Factory.deserialize({"name": factory_name, "id": factory_id})
    ]

    def mock_list_datasets(factory_name, resource_group_name):
        return [
            DfModels.DatasetResource.deserialize(
                {
                    "id": "001",
                    "name": "dataset_1",
                    "properties": {
                        "type": "SnowflakeTable",
                        "linkedServiceName": {
                            "referenceName": "snowflake_linked_service"
                        },
                        "typeProperties": {
                            "schema": "SCHEMA",
                            "table": "TABLE1",
                        },
                    },
                }
            ),
            DfModels.DatasetResource.deserialize(
                {
                    "id": "002",
                    "name": "dataset_2",
                    "properties": {
                        "type": "SnowflakeTable",
                        "linkedServiceName": {
                            "referenceName": "snowflake_linked_service"
                        },
                        "typeProperties": {
                            "schema": "SCHEMA",
                            "table": "TABLE2",
                        },
                    },
                }
            ),
            DfModels.DatasetResource.deserialize(
                {
                    "id": "003",
                    "name": "dataset_3",
                    "properties": {
                        "type": "AzureSqlTable",
                        "linkedServiceName": {
                            "referenceName": "sql_database_linked_service"
                        },
                        "typeProperties": {
                            "schema": "adf",
                            "table": "example",
                        },
                    },
                }
            ),
        ]

    mock_client.datasets.list_by_factory.side_effect = mock_list_datasets

    def mock_list_linked_services(factory_name, resource_group_name):
        return [
            DfModels.LinkedServiceResource.deserialize(
                {
                    "name": "snowflake_linked_service",
                    "properties": {
                        "type": "Snowflake",
                        "typeProperties": {
                            "connectionString": "jdbc:snowflake://snowflake_account.snowflakecomputing.com/?user=user&db=DATABASE&warehouse=WH&role=role",
                        },
                    },
                }
            ),
            DfModels.LinkedServiceResource.deserialize(
                {
                    "name": "sql_database_linked_service",
                    "properties": {
                        "type": "AzureSqlDatabase",
                        "typeProperties": {
                            "connectionString": "Integrated Security=False;Encrypt=True;Connection Timeout=30;Data Source=sql-server-host.database.windows.net;Initial Catalog=main-db",
                        },
                    },
                }
            ),
        ]

    mock_client.linked_services.list_by_factory.side_effect = mock_list_linked_services

    def mock_list_data_flows(factory_name, resource_group_name):
        return [
            DfModels.DataFlowResource.deserialize(
                {
                    "name": "data_flow_1",
                    "properties": {
                        "type": "MappingDataFlow",
                        "typeProperties": {
                            "sources": [{"dataset": {"referenceName": "dataset_1"}}],
                            "sinks": [{"dataset": {"referenceName": "dataset_2"}}],
                        },
                    },
                }
            )
        ]

    mock_client.data_flows.list_by_factory.side_effect = mock_list_data_flows

    def mock_list_pipelines(factory_name, resource_group_name):
        return [
            DfModels.PipelineResource.deserialize(
                {
                    "name": "pipeline-1",
                    "id": "pipeline-id-1",
                    "properties": {
                        "activities": [
                            {
                                "name": "Data flow1",
                                "type": "ExecuteDataFlow",
                                "dependsOn": [],
                                "userProperties": [],
                                "typeProperties": {
                                    "dataFlow": {
                                        "referenceName": "data_flow_1",
                                        "type": "DataFlowReference",
                                    },
                                },
                            },
                            {
                                "name": "Copy data1",
                                "type": "Copy",
                                "dependsOn": [
                                    {
                                        "activity": "Data flow1",
                                        "dependencyConditions": ["Succeeded"],
                                    }
                                ],
                                "userProperties": [],
                                "inputs": [
                                    {
                                        "referenceName": "dataset_2",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "outputs": [
                                    {
                                        "referenceName": "dataset_3",
                                        "type": "DatasetReference",
                                    }
                                ],
                            },
                        ]
                    },
                }
            )
        ]

    mock_client.pipelines.list_by_factory.side_effect = mock_list_pipelines

    def mock_get_pipeline_runs(factory_name, resource_group_name, filter_parameters):
        return DfModels.PipelineRunsQueryResponse(value=[])

    mock_client.pipeline_runs.query_by_factory.side_effect = mock_get_pipeline_runs
    mock_client.pipeline_runs._serialize.body.side_effect = lambda _1, _2: {
        "filters": []
    }

    mock_build_client.side_effect = lambda _: mock_client

    config = AzureDataFactoryRunConfig(
        output=OutputConfig(),
        tenant_id="fake",
        client_id="fake_client_id",
        client_secret="face_client_secret",
        subscription_id="fact_subscription_id",
    )
    extractor = AzureDataFactoryExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    import json

    print(json.dumps(events, indent=2))
    assert events == load_json(f"{test_root_dir}/azure_data_factory/expected.json")


def test_get_resource_group():
    factory_id = (
        "/subscriptions/<sub_id>/resourceGroups/<resource_group>/providers/Microsoft.DataFactory/factories/<name>",
    )
    assert (
        AzureDataFactoryExtractor._get_resource_group_from_factory(
            DfModels.Factory.deserialize({"id": factory_id})
        )
        == "<resource_group>"
    )
