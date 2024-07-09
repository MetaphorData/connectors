from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class MondayRunConfig(BaseConfig):
    # Monday integration authorization key
    monday_api_key: str

    # Monday API version
    monday_api_version: str

    # Embeddings source
    embed_source: str = "azure"

    # Azure OpenAI configs
    azure_openAI_key: str = ""
    azure_openAI_endpoint: str = ""

    # OpenAI configs
    openAI_key: str = ""

    # Default Azure OpenAI services configs
    azure_openAI_version: str = "2024-03-01-preview"
    azure_openAI_model: str = "text-embedding-3-small"
    azure_openAI_model_name: str = "Embedding_3_small"
    openAI_model: str = "text-embedding-3-small"

    # Store the document's content alongside embeddings
    include_text: bool = False
