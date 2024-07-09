from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class StaticWebRunConfig(BaseConfig):
    # Top-level URLs to scrape content from
    links: list

    # Configurable scraping depth
    depths: list

    # Embeddings source
    embed_source: str = "azure"

    # Azure OpenAI configs
    azure_openAI_key: str = ""
    azure_openAI_endpoint: str = ""

    # OpenAI configs
    openAI_key: str = ""

    # Default AI configs
    azure_openAI_version: str = "2024-03-01-preview"
    azure_openAI_model: str = "text-embedding-3-small"
    azure_openAI_model_name: str = "Embedding_3_small"
    openAI_model: str = "text-embedding-3-small"

    # Store the document's content alongside embeddings
    include_text: bool = True
