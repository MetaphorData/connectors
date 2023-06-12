from dataclasses import field
from typing import List, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class PowerBIRunConfig(BaseConfig):
    # Power BI Directory (tenant) ID
    tenant_id: str

    # Azure AD application client ID
    client_id: str

    # Azure AD Application client secret
    secret: str

    # (Optional) The ids of Power BI workspace
    workspaces: List[str] = field(default_factory=lambda: list())

    # (Optional) The default snowflake account
    snowflake_account: Optional[str] = None
