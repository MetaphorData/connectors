from dataclasses import asdict, field

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

    # insert user-provided embedding model configs
    def __post_init__(self):
        default_config = EmbeddingModelConfig()
        self.embedding_model.update(asdict(default_config))
