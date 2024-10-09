from dataclasses import field
from datetime import datetime

from pydantic import field_validator
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

    @field_validator("notion_api_version", mode="before")
    def ensure_version_is_string(cls, value):
        """
        Handle the case where the version is a datetime object.
        """
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        return str(value)
