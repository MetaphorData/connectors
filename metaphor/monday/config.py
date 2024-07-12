from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig


@dataclass(config=ConnectorConfig)
class MondayRunConfig(BaseConfig):
    # Monday integration authorization key
    monday_api_key: str

    # Monday API version
    monday_api_version: str

    # Embeddings
    embedding_model: EmbeddingModelConfig = field(default_factory=EmbeddingModelConfig)

    # Store the document's content alongside embeddings
    include_text: bool = False
