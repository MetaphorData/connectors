from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class SharepointRunConfig(BaseConfig):
    # General MS Entra auth configs
    client_id: str
    client_seceret: str
    tenant_id: str

    # Azure OpenAI configs
    azure_openAI_key: str
    azure_openAI_endpoint: str

    # Store the document's content alongside embeddings
    include_text: bool = False