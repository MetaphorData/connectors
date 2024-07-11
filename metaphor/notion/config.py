from dataclasses import asdict, field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig


@dataclass(config=ConnectorConfig)
class NotionRunConfig(BaseConfig):
    # Notion integration authorization token
    notion_api_token: str

    # Embeddings
    embedding_model: EmbeddingModelConfig = field(default_factory=EmbeddingModelConfig)

    # Store the document's content alongside embeddings
    include_text: bool = False

    # Notion API version
    notion_api_version: str = "2022-06-28"

    # insert user-provided embedding model configs
    def __post_init__(self):
        default_config = EmbeddingModelConfig()
        self.embedding_model.update(asdict(default_config))
