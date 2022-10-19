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
