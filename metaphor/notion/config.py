from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class NotionRunConfig(BaseConfig):
    # notion integration authorization token
    notion_api_tok: str

    # openai token
    openai_api_tok: str

    # Include embeddingString in nodes stored to db
    include_text: bool

    # Notion API version
    notion_api_version: Optional[str] = "2022-06-08"
