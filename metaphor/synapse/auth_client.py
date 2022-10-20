from typing import Iterable, List

from metaphor.common.api_request import get_request
from metaphor.common.logger import get_logger
from metaphor.synapse.config import SynapseConfig
from metaphor.synapse.workspace_client import SynapseWorkspace, WorkspaceClient

try:
    import msal
except ImportError:
    print("Please install metaphor[synapse] extra\n")
    raise

logger = get_logger(__name__)


class AuthClient:
    AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
    AZURE_SYNAPSE_SCOPES = ["https://dev.azuresynapse.net/.default"]
    AZURE_MANGEMENT_SCOPES = ["https://management.azure.com/.default"]
    AZURE_MANGEMENT_ENDPOINT = "https://management.azure.com"
    AZURE_STORAGE_SCOPES = ["https://storage.azure.com/.default"]

    def __init__(self, config: SynapseConfig):
        self._subscription_id = config.subscription_id
        self.workspace_names = config.workspaces
        self.resource_group_name = config.resource_group_name
        self._azure_management_headers = {
            "Authorization": self.retrieve_access_token(
                config, self.AZURE_MANGEMENT_SCOPES
            )
        }
        self._azure_synapse_headers = {
            "Authorization": self.retrieve_access_token(
                config, self.AZURE_SYNAPSE_SCOPES
            )
        }

        self._azure_storage_headers = {
            "Authorization": self.retrieve_access_token(
                config, self.AZURE_STORAGE_SCOPES
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

    def _get_workspace(self, workspace_name: str) -> WorkspaceClient:
        # https://learn.microsoft.com/en-us/rest/api/synapse/workspaces/get?tabs=HTTP
        url = f"{self.AZURE_MANGEMENT_ENDPOINT}/subscriptions/{self._subscription_id}/resourceGroups/{self.resource_group_name}/providers/Microsoft.Synapse/workspaces/{workspace_name}?api-version=2021-06-01"
        return get_request(
            url,
            self._azure_management_headers,
            SynapseWorkspace,
            transform_response=lambda r: r.json(),
        )

    def _get_workspaces(self) -> List[SynapseWorkspace]:
        # https://learn.microsoft.com/en-us/rest/api/synapse/workspaces/list?tabs=HTTP
        url = f"{self.AZURE_MANGEMENT_ENDPOINT}/subscriptions/{self._subscription_id}/providers/Microsoft.Synapse/workspaces?api-version=2021-06-01"
        return get_request(
            url,
            self._azure_management_headers,
            List[SynapseWorkspace],
            transform_response=lambda r: r.json()["value"],
        )

    def get_list_workspace_clients(self) -> Iterable[WorkspaceClient]:
        workspaces: List[SynapseWorkspace] = []
        if self.resource_group_name and len(self.workspace_names) > 0:
            workspaces = [
                self._get_workspace(workspace_name)
                for workspace_name in self.workspace_names
            ]
        else:
            workspaces = self._get_workspaces()

        for workspace in workspaces:
            yield WorkspaceClient(
                workspace,
                self._subscription_id,
                self._azure_synapse_headers,
                self._azure_management_headers,
                self._azure_storage_headers,
            )
