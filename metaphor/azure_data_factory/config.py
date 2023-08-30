from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class AzureDataFactoryRunConfig(BaseConfig):
    # Azure Directory (tenant) ID
    tenant_id: str = ""

    # Azure client ID (App ID)
    client_id: str = ""

    # Azure client secret
    client_secret: str = ""

    # Azure subscription id
    subscription_id: str = ""
