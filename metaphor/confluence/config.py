from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig


@dataclass(config=ConnectorConfig)
class ConfluenceRunConfig(BaseConfig):
    # General Confluence configs
    confluence_base_URL: str
    confluence_cloud: bool
    select_method: str

    # Embeddings
    embedding_model: EmbeddingModelConfig = field(default_factory=EmbeddingModelConfig)

    # Confluence username / token (Cloud)
    confluence_username: str = ""
    confluence_token: str = ""

    # Confluence PAT (Data Center / Server)
    confluence_PAT: str = ""

    # Selection method
    space_key: str = ""
    page_ids: list = field(default_factory=list)
    label: str = ""
    cql: str = ""

    # Store the document's content alongside embeddings
    include_text: bool = False

    # Parse document attachments
    include_attachments: bool = False

    # Parse page children (when page_ids used)
    include_children: bool = False

    # Filter by page status (when space_key used)
    page_status: str = ""
