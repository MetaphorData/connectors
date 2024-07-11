import os
from datetime import datetime, timezone
from typing import Collection, List

from llama_index.core import Document
from llama_index.readers.confluence import ConfluenceReader

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.confluence.config import ConfluenceRunConfig
from metaphor.models.crawler_run_metadata import Platform

logger = get_logger()


class ConfluenceExtractor(BaseExtractor):
    """Confluence Page Extractor."""

    _description = "Extracts pages from a Confluence instance."
    _platform = Platform.CONFLUENCE

    @staticmethod
    def from_config_file(config_file: str) -> "ConfluenceExtractor":
        return ConfluenceExtractor(ConfluenceRunConfig.from_yaml_file(config_file))

    def __init__(self, config: ConfluenceRunConfig):
        super().__init__(config=config)  # type: ignore[call-arg]

        # Confluence instance setup
        self.confluence_base_URL = config.confluence_base_URL
        self.confluence_cloud = config.confluence_cloud
        self.select_method = config.select_method

        # Authentication
        self.confluence_username = config.confluence_username
        self.confluence_password = config.confluence_token
        self.confluence_PAT = config.confluence_PAT

        # Selection
        self.space_key = config.space_key
        self.page_ids = config.page_ids
        self.label = config.label
        self.cql = config.cql

        # Configs
        self.include_text = config.include_text
        self.include_attachments = config.include_attachments
        self.include_children = config.include_children
        self.page_status = config.page_status

        # Embedding source and configs
        self.embedding_model = config.embedding_model

        # Replace empty configs for validation
        self.space_key = self.space_key if self.space_key else None  # type: ignore[assignment]
        self.page_ids = self.page_ids if self.page_ids else None  # type: ignore[assignment]
        self.label = self.label if self.label else None  # type: ignore[assignment]
        self.cql = self.cql if self.cql else None  # type: ignore[assignment]

        # Set appropriate environment variables
        if self.confluence_cloud:
            os.environ["CONFLUENCE_USERNAME"] = self.confluence_username
            os.environ["CONFLUENCE_PASSWORD"] = self.confluence_password
        else:
            os.environ["CONFLUENCE_API_TOKEN"] = self.confluence_PAT

        # Initialize Reader; will read appropriate environment variables.
        self.confluence_reader = ConfluenceReader(
            base_url=self.confluence_base_URL, cloud=self.confluence_cloud
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching pages from Confluence: {self.confluence_base_URL}")

        documents = self._load_confluence_data()

        logger.info("Starting embedding process")

        vector_store_index = embed_documents(
            docs=documents,
            embedding_model=self.embedding_model,
        )

        embedded_nodes = map_metadata(
            vector_store_index, include_text=self.include_text
        )

        return embedded_nodes

    def _load_confluence_data(self) -> List[Document]:
        # Use ConfluenceReader to extract Documents.
        # ConfluenceReader validates that only one input type is present.
        docs = self.confluence_reader.load_data(
            space_key=self.space_key,
            page_ids=self.page_ids,
            page_status=self.page_status,
            label=self.label,
            cql=self.cql,
            include_attachments=self.include_attachments,
            include_children=self.include_children,
        )

        current_time = str(datetime.now(timezone.utc))

        for doc in docs:
            # Reset page_id
            doc.metadata["pageId"] = doc.metadata.pop("page_id")

            # Clean the document text
            doc.text = sanitize_text(doc.text)

            # Update platform
            doc.metadata["platform"] = "confluence"

            # Reset link
            doc.metadata["link"] = doc.metadata.pop("url")

            # Add timestamp
            doc.metadata["lastRefreshed"] = current_time

        logger.info(f"Successfully retrieved {len(docs)} documents.")

        return docs
