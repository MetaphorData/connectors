from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class PowerBIRunConfig(BaseConfig):
    # Power BI Directory (tenant) ID
    tenant_id: str

    # Power BI workspace id, if left empty the connector will walk through all workspaces.
    workspace_id: str

    # Azure AD application client ID
    client_id: str

    # Azure AD Application client secret
    secret: str
