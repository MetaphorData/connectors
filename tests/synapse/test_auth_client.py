from unittest.mock import MagicMock, patch

from metaphor.common.base_config import OutputConfig
from metaphor.synapse.auth_client import AuthClient, SynapseConfig, SynapseWorkspace

app = MagicMock()

synapseWorkspace = SynapseWorkspace(
    id="mock_workspace_id/resourceGroups/mock_resource_group/providers/",
    name="mock_workspace",
    type="WORKSPACE",
    properties={
        "Origin": {"Type": "mock_database_type"},
        "connectivityEndpoints": {
            "dev": "mock_workspace-dev.synapse.net",
            "sql": "mock_workspace-dev.sql.synapse.net",
            "sqlOnDemand": "mock_workspace-dev-on-demand.sql.synapse.net",
        },
        "defaultDataLakeStorage": {
            "accountUrl": "mock_accunt_url.synapse.net",
            "filesystem": "mock_data_lake_storage",
        },
    },
)


def test_get_workspace_client():
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
                workspace_name="mock_workspace",
                resource_group_name="resource_group_name",
                output=OutputConfig(file={"directory": "./synapse_result"}),
            )
        )
        with patch(
            "metaphor.synapse.auth_client.get_request", return_value=synapseWorkspace
        ):
            workspaceClient = authClient.get_workspace_client()
            assert workspaceClient._workspace == synapseWorkspace
