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
        ]

    mock_client.linked_services.list_by_factory.side_effect = mock_list_linked_services

    def mock_list_data_flows(factory_name, resource_group_name):
        return [
            DfModels.DataFlowResource.deserialize(
                {
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
