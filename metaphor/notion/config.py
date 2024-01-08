from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class NotionRunConfig(BaseConfig):
    # Notion integration authorization token
    notion_api_token: str

    # Azure OpenAI services configs
    azure_openAI_key: str
    azure_openAI_version: str
    azure_openAI_endpoint: str
    azure_openAI_model: str
    azure_openAI_model_name: str

    # Store the document's content alongside embeddings
    include_text: bool = False

    # Notion API version
    notion_api_version: Optional[str] = "2022-06-08"
