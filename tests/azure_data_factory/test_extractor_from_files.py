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
        datasets = load_json(f"{test_root_dir}/azure_data_factory/data/datasets.json")
        return [DfModels.DatasetResource.deserialize(d) for d in datasets]

    mock_client.datasets.list_by_factory.side_effect = mock_list_datasets

    def mock_list_linked_services(factory_name, resource_group_name):
        linked_services = load_json(
            f"{test_root_dir}/azure_data_factory/data/linked_services.json"
        )
        return [DfModels.LinkedServiceResource.deserialize(s) for s in linked_services]

    mock_client.linked_services.list_by_factory.side_effect = mock_list_linked_services

    def mock_list_data_flows(factory_name, resource_group_name):
        data_flows = load_json(
            f"{test_root_dir}/azure_data_factory/data/data_flows.json"
        )
        return [DfModels.DataFlowResource.deserialize(f) for f in data_flows]

    mock_client.data_flows.list_by_factory.side_effect = mock_list_data_flows

    def mock_list_pipelines(factory_name, resource_group_name):
        pipelines = load_json(f"{test_root_dir}/azure_data_factory/data/pipelines.json")
        return [DfModels.PipelineResource.deserialize(p) for p in pipelines]

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

    assert events == load_json(
        f"{test_root_dir}/azure_data_factory/expected_files.json"
    )
