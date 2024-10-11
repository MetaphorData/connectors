from dataclasses import field
from typing import List, Optional

from pydantic import model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.common.utils import must_set_exactly_one


@dataclass(config=ConnectorConfig)
class ConfluenceRunConfig(BaseConfig):
    # General Confluence configs
    confluence_base_URL: str
    confluence_cloud: bool = True
    select_method: Optional[str] = None  # TODO: remove at next breaking change release

    # Embeddings
    embedding_model: EmbeddingModelConfig = field(default_factory=EmbeddingModelConfig)

    # Confluence username / token (Cloud)
    confluence_username: Optional[str] = None
    confluence_token: Optional[str] = None

    # Confluence PAT (Data Center / Server)
    confluence_PAT: Optional[str] = None

    # Selection method
    space_key: Optional[str] = None
    space_keys: Optional[List[str]] = None
    page_ids: Optional[List[str]] = None
    label: Optional[str] = None
    cql: Optional[str] = None

    # Store the document's content alongside embeddings
    include_text: bool = False

    # Parse document attachments
    include_attachments: bool = False

    # Parse page children (when page_ids used)
    include_children: bool = False

    # Filter by page status (when space_key used)
    page_status: str = ""

    @model_validator(mode="after")
    def validate_confluence_config(self) -> "ConfluenceRunConfig":
        # Validate credentials
        if self.confluence_cloud:
            if not self.confluence_username or not self.confluence_token:
                raise ValueError(
                    "confluence_username and confluence_token must be provided for Confluence Cloud"
                )
        else:
            if not self.confluence_PAT:
                raise ValueError(
                    "confluence_PAT must be provided for Confluence Data Center/Server"
                )

        # Validate selection
        must_set_exactly_one(
            self.__dict__, ["space_key", "space_keys", "page_ids", "label", "cql"]
        )

        return self
