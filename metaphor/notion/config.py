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

    # Mongo URI for documents
    mongo_uri: str
    mongo_db_name: str
    mongo_collection_name: str

    # Notion API version
    notion_api_version: Optional[str] = "2022-06-08"

    # embedding node size configuration
    embedding_chunk_size: Optional[int] = 512
    embedding_overlap_size: Optional[int] = 50