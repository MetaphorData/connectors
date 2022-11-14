from dataclasses import field
from typing import List, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class SynapseConfig(BaseConfig):
    # Azure Directory (tenant) ID
    tenant_id: str

    # Azure AD application client ID
    client_id: str

    # Azure AD Application client secret
    secret: str

    # The Azure Subscription ID
    subscription_id: str

    # (Optinal) The Rescource Group Name
    resource_group_name: Optional[str] = None

    # (Optional) The workspace names, need to set resource_group_name
    workspaces: List[str] = field(default_factory=lambda: list())

    # (Optional) The  username for query logs
    username: Optional[str] = None

    # (Optional) The password for query logs
    password: Optional[str] = None

    # (Optional) Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: Optional[int] = 0
