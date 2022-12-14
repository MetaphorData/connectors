from metaphor.common.api_request import get_request
from metaphor.common.logger import get_logger
from metaphor.synapse.config import SynapseConfig
from metaphor.synapse.workspace_client import SynapseWorkspace, WorkspaceClient

try:
    import msal
except ImportError:
    print("Please install metaphor[synapse] extra\n")
    raise

logger = get_logger()


class AuthClient:
    AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
    AZURE_SYNAPSE_SCOPES = ["https://dev.azuresynapse.net/.default"]
    AZURE_MANGEMENT_SCOPES = ["https://management.azure.com/.default"]
    AZURE_MANGEMENT_ENDPOINT = "https://management.azure.com"

    def __init__(self, config: SynapseConfig):
        self._tenant_id = config.tenant_id
        self._subscription_id = config.subscription_id
        self._workspace_name = config.workspace_name
        self._db_username = config.username
        self._db_password = config.password
        self._resource_group_name = config.resource_group_name
        self._azure_management_headers = {
            "Authorization": self.retrieve_access_token(
                config, self.AZURE_MANGEMENT_SCOPES
            )
        }

    def retrieve_access_token(self, config: SynapseConfig, scopes) -> str:
        app = msal.ConfidentialClientApplication(
            config.client_id,
            authority=self.AUTHORITY.format(tenant_id=config.tenant_id),
            client_credential=config.secret,
        )
        token = None
        token = app.acquire_token_silent(scopes, account=None)

        if not token:
            logger.info(
                "No suitable token exists in cache. Let's get a new one from AAD."
            )
            token = app.acquire_token_for_client(scopes=scopes)
        return f"Bearer {token['access_token']}"

    def _get_workspace(self) -> WorkspaceClient:
        # https://learn.microsoft.com/en-us/rest/api/synapse/workspaces/get?tabs=HTTP
        url = f"{self.AZURE_MANGEMENT_ENDPOINT}/subscriptions/{self._subscription_id}/resourceGroups/{self._resource_group_name}/providers/Microsoft.Synapse/workspaces/{self._workspace_name}?api-version=2021-06-01"
        return get_request(
            url,
            self._azure_management_headers,
            SynapseWorkspace,
            transform_response=lambda r: r.json(),
        )

    def get_workspace_client(self) -> WorkspaceClient:
        workspace = self._get_workspace()
        return WorkspaceClient(
            workspace,
            self._subscription_id,
            self._db_username,
            self._db_password,
            self._azure_management_headers,
        )
