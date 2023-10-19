from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.power_bi.config import PowerBIRunConfig
from metaphor.power_bi.power_bi_client import (
    PowerBIActivityEventEntity,
    PowerBIClient,
    PowerBiRefreshSchedule,
    PowerBISubscription,
)
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


@patch("requests.get")
@patch("requests.post")
@patch("msal.ConfidentialClientApplication")
@pytest.mark.asyncio
async def test_get_workspace_info(
    mock_msal_app: MagicMock,
    mock_post_method: MagicMock,
    mock_get_method: MagicMock,
    test_root_dir: str,
):
    mock_msal_app = MagicMock()
    mock_msal_app.acquire_token_silent = MagicMock(
        return_value={"access_token": "token"}
    )

    mock_post_method.side_effect = [
        MockResponse({"id": "scan_id"}, 202),
    ]

    mock_get_method.side_effect = [
        MockResponse({"status": "Processing"}),
        MockResponse({"status": "Succeeded"}),
        MockResponse(load_json(f"{test_root_dir}/power_bi/data/workspace_scan.json")),
    ]
    client = PowerBIClient(
        PowerBIRunConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            secret="secret",
            output=OutputConfig(),
        )
    )

    workspaces = client.get_workspace_info([])

    # Verified modeling was correct
    assert len(workspaces) == 1


@patch("requests.get")
@patch("msal.ConfidentialClientApplication")
@pytest.mark.asyncio
async def test_get_activities(
    mock_msal_app: MagicMock, mock_get_method: MagicMock, test_root_dir: str
):
    mock_msal_app = MagicMock()
    mock_msal_app.acquire_token_silent = MagicMock(
        return_value={"access_token": "token"}
    )

    mock_get_method.side_effect = [
        MockResponse(load_json(f"{test_root_dir}/power_bi/data/activities_1.json")),
        MockResponse(load_json(f"{test_root_dir}/power_bi/data/activities_2.json")),
        MockResponse(load_json(f"{test_root_dir}/power_bi/data/activities_3.json")),
    ]
    client = PowerBIClient(
        PowerBIRunConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            secret="secret",
            output=OutputConfig(),
        )
    )

    activities = client.get_activities(0)

    assert activities == [
        PowerBIActivityEventEntity(**activity)
        for activity in load_json(
            f"{test_root_dir}/power_bi/data/expected_activities.json"
        )
    ]


@patch("requests.get")
@patch("msal.ConfidentialClientApplication")
@pytest.mark.asyncio
async def test_get_refresh_schedule(
    mock_msal_app: MagicMock, mock_get_method: MagicMock, test_root_dir: str
):
    mock_msal_app = MagicMock()
    mock_msal_app.acquire_token_silent = MagicMock(
        return_value={"access_token": "token"}
    )

    mock_get_method.side_effect = [
        MockResponse(
            load_json(f"{test_root_dir}/power_bi/data/dataset_refresh_schedule.json")
        ),
    ]
    client = PowerBIClient(
        PowerBIRunConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            secret="secret",
            output=OutputConfig(),
        )
    )

    refresh_schedule = client.get_refresh_schedule("", "")

    assert refresh_schedule == PowerBiRefreshSchedule(
        frequency=None,
        days=[
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ],
        times=["10:30"],
        enabled=False,
        localTimeZoneId="UTC",
        notifyOption="MailOnFailure",
    )


@patch("requests.get")
@patch("msal.ConfidentialClientApplication")
@pytest.mark.asyncio
async def test_get_direct_query_refresh_schedule(
    mock_msal_app: MagicMock, mock_get_method: MagicMock, test_root_dir: str
):
    mock_msal_app = MagicMock()
    mock_msal_app.acquire_token_silent = MagicMock(
        return_value={"access_token": "token"}
    )

    mock_get_method.side_effect = [
        MockResponse(
            load_json(
                f"{test_root_dir}/power_bi/data/direct_query_refresh_schedule.json"
            )
        ),
    ]
    client = PowerBIClient(
        PowerBIRunConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            secret="secret",
            output=OutputConfig(),
        )
    )

    refresh_schedule = client.get_direct_query_refresh_schedule("", "")

    assert refresh_schedule == PowerBiRefreshSchedule(
        frequency="60",
        days=[],
        times=[],
        enabled=None,
        localTimeZoneId="UTC",
        notifyOption=None,
    )
