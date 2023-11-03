from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from msgraph_beta.generated.models.o_data_errors.o_data_error import ODataError
from msgraph_beta.generated.models.security.sensitivity_label import SensitivityLabel

from metaphor.common.base_config import OutputConfig
from metaphor.models.metadata_change_event import PowerBISensitivityLabel
from metaphor.power_bi.config import PowerBIRunConfig
from metaphor.power_bi.graph_api_client import GraphApiClient


async def mock_get_sensitivity_label():
    return None


def mock_graph_client(mocked_graph_client) -> AsyncMock:
    mocked_graph_client_instance = MagicMock()
    mocked_builder = MagicMock()
    mocked_graph_client_instance.security.information_protection.sensitivity_labels.by_sensitivity_label_id = (
        mocked_builder
    )
    mocked_builder_instance = MagicMock()
    mocked_builder.return_value = mocked_builder_instance

    return_mock = AsyncMock()

    mocked_builder_instance.get = return_mock

    mocked_graph_client.return_value = mocked_graph_client_instance

    return return_mock


dummy_config = PowerBIRunConfig(
    tenant_id="tenant-id",
    client_id="client-id",
    secret="secret",
    output=OutputConfig(),
)


@patch("metaphor.power_bi.graph_api_client.GraphServiceClient")
@pytest.mark.asyncio
async def test_get_labels(mocked_graph_client: MagicMock, test_root_dir: str):
    mocked_method = mock_graph_client(mocked_graph_client)

    client = GraphApiClient(dummy_config)

    mocked_method.return_value = None
    label = await client.get_labels("label_id")
    assert label is None

    mocked_method.return_value = SensitivityLabel(id="foo")
    label = await client.get_labels("label_id")
    assert label == PowerBISensitivityLabel(id="label_id")

    # Cache hit
    mocked_method.return_value = None
    label = await client.get_labels("label_id")
    assert label == PowerBISensitivityLabel(id="label_id")

    # None label_id
    label = await client.get_labels(None)
    assert label is None

    # Throw error
    mocked_method.side_effect = ODataError()
    with pytest.raises(ODataError):
        label = await client.get_labels("foo")

    # 403 error
    mocked_method.side_effect = ODataError(response_status_code=403)
    label = await client.get_labels("foo")
    assert label is None
