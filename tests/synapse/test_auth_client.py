from unittest.mock import MagicMock, patch

from metaphor.common.base_config import OutputConfig
from metaphor.synapse.auth_client import (
    ApiReqest,
    AuthClient,
    SynapseConfig,
    SynapseWorkspace,
)

app = MagicMock()

synapseWorkspace1 = SynapseWorkspace(
    id="mock_database_id/resourceGroups/mock_resource_group/providers/",
    name="workspace1",
    type="WORKSPACE",
    properties={
        "Origin": {"Type": "mock_database_type"},
        "connectivityEndpoints": {
            "dev": "workspace1-dev.synapse.net",
            "sql": "workspace1-dev.sql.synapse.net",
            "sqlOnDemand": "workspace1-dev-on-demand.sql.synapse.net",
        },
        "defaultDataLakeStorage": {
            "accountUrl": "mock_accunt_url.synapse.net",
            "filesystem": "mock_data_lake_storage",
        },
    },
)

synapseWorkspace2 = SynapseWorkspace(
    id="mock_database_id/resourceGroups/mock_resource_group/providers/",
    name="workspace2",
    type="WORKSPACE",
    properties={
        "Origin": {"Type": "mock_database_type"},
        "connectivityEndpoints": {
            "dev": "workspace2-dev.synapse.net",
            "sql": "workspace2-dev.sql.synapse.net",
            "sqlOnDemand": "workspace2-dev-on-demand.sql.synapse.net",
        },
        "defaultDataLakeStorage": {
            "accountUrl": "mock_accunt_url.synapse.net",
            "filesystem": "mock_data_lake_storage",
        },
    },
)


def test_get_list_workspace_clients_with_assigned_workspaces():
    with patch(
        "metaphor.synapse.auth_client.msal.ConfidentialClientApplication"
    ) as mockClient:
        mockClient.return_value = app
        app.acquire_token_silent = MagicMock(
            return_value={"access_token": "mock_token"}
        )
        authClient = AuthClient(
            SynapseConfig(
                tenant_id="tenant_id",
                client_id="client_id",
                secret="client_secret",
                subscription_id="client_subscription_id",
                workspaces=["workspace1", "workspace2"],
                resource_group_name="resource_group_name",
                output=OutputConfig(file={"directory": "./synapse_result"}),
            )
        )
        with patch.object(
            ApiReqest, "get_request", side_effect=[synapseWorkspace1, synapseWorkspace2]
        ):
            workspaceClients = authClient.get_list_workspace_clients()
            assert next(workspaceClients)._workspace == synapseWorkspace1
            assert next(workspaceClients)._workspace == synapseWorkspace2


def test_get_list_workspace_clients():
    with patch(
        "metaphor.synapse.auth_client.msal.ConfidentialClientApplication"
    ) as mockClient:
        mockClient.return_value = app
        app.acquire_token_silent = MagicMock(
            return_value={"access_token": "mock_token"}
        )
        authClient = AuthClient(
            SynapseConfig(
                tenant_id="tenant_id",
                client_id="client_id",
                secret="client_secret",
                subscription_id="client_subscription_id",
                output=OutputConfig(file={"directory": "./synapse_result"}),
            )
        )
        with patch.object(
            ApiReqest,
            "get_request",
            return_value=[synapseWorkspace1, synapseWorkspace2],
        ):
            workspaceClients = authClient.get_list_workspace_clients()
            assert next(workspaceClients)._workspace == synapseWorkspace1
            assert next(workspaceClients)._workspace == synapseWorkspace2
