from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class NotionRunConfig(BaseConfig):
    # notion integration authorization token
    notion_api_tok: str

    # azure openai services configs
    azure_openAI_key: str
    azure_openAI_ver: str
    azure_openAI_endpoint: str
    azure_openAI_model: str
    azure_openAI_model_name: str

    # Include embeddingString in nodes stored to db
    include_text: bool

    # Notion API version
    notion_api_version: Optional[str] = "2022-06-08"
