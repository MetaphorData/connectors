from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.power_bi.config import PowerBIRunConfig
from metaphor.power_bi.power_bi_client import PowerBIClient, PowerBISubscription
from tests.test_utils import load_json


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return


@patch("requests.get")
@patch("msal.ConfidentialClientApplication")
@pytest.mark.asyncio
async def test_get_user_subscriptions(
    mock_msal_app: MagicMock, mock_get_method: MagicMock, test_root_dir: str
):
    mock_msal_app = MagicMock()
    mock_msal_app.acquire_token_silent = MagicMock(
        return_value={"access_token": "token"}
    )

    mock_get_method.side_effect = [
        MockResponse(
            load_json(f"{test_root_dir}/power_bi/data/user_subscriptions.json")
        ),
        MockResponse({"SubscriptionEntities": []}),
    ]
    client = PowerBIClient(
        PowerBIRunConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            secret="secret",
            output=OutputConfig(),
        )
    )

    subscriptions = client.get_user_subscriptions("user_id")
    assert len(subscriptions) == 1
    assert (
        subscriptions[0].dict()
        == PowerBISubscription(
            id="some-uuid",
            artifactId="dashboard-uuid",
            title="Subscription title",
            frequency="Daily",
            endDate="9/12/2024 12:00:00 AM",
            startDate="9/12/2023 12:00:00 AM",
            artifactDisplayName="Report Name",
            subArtifactDisplayName="Page Name",
            users=[],
        ).dict()
    )


@patch("requests.get")
@patch("msal.ConfidentialClientApplication")
@pytest.mark.asyncio
async def test_get_export_dataflow(
    mock_msal_app: MagicMock, mock_get_method: MagicMock, test_root_dir: str
):
    mock_msal_app = MagicMock()
    mock_msal_app.acquire_token_silent = MagicMock(
        return_value={"access_token": "token"}
    )

    mock_get_method.side_effect = [
        MockResponse(load_json(f"{test_root_dir}/power_bi/data/dataflow_1.json")),
    ]
    client = PowerBIClient(
        PowerBIRunConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            secret="secret",
            output=OutputConfig(),
        )
    )

    dataflow = client.export_dataflow("group_id", "dataflow_id")
    assert dataflow == load_json(f"{test_root_dir}/power_bi/data/dataflow_1.json")
