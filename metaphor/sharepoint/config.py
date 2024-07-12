from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig


@dataclass(config=ConnectorConfig)
class SharepointRunConfig(BaseConfig):
    # General Azure API auth configs
    sharepoint_client_id: str
    sharepoint_client_secret: str
    sharepoint_tenant_id: str

    # Embeddings
    embedding_model: EmbeddingModelConfig = field(default_factory=EmbeddingModelConfig)

    # Store the document's content alongside embeddings
    include_text: bool = False
