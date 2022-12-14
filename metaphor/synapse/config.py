from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class SynapseQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 0


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

    # Synapse Workspace name
    workspace_name: str

    # The Rescource Group Name
    resource_group_name: str

    # The database username
    username: str

    # The database password
    password: str

    query_log: Optional[SynapseQueryLogConfig] = SynapseQueryLogConfig()
