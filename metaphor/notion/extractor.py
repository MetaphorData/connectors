import datetime
import json
from typing import Collection, Sequence

import requests
from llama_index.core import Document
from llama_index.readers.notion import NotionPageReader
from requests.exceptions import HTTPError

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.notion.config import NotionRunConfig

logger = get_logger()

baseURL = "https://api.notion.com/v1"


class NotionExtractor(BaseExtractor):
    """Notion Document extractor."""

    _description = "Notion document crawler"
    _platform = Platform.UNKNOWN

    @staticmethod
    def from_config_file(config_file: str) -> "NotionExtractor":
        return NotionExtractor(NotionRunConfig.from_yaml_file(config_file))

    def __init__(self, config: NotionRunConfig):
        super().__init__(config=config)  # type: ignore[call-arg]

        self.notion_api_token = config.notion_api_token
        self.notion_api_version = config.notion_api_version

        # Embedding source and configs
        self.embedding_model = config.embedding_model

        self.include_text = config.include_text

        # Set up LlamaIndex Notion integration
        self.NotionReader = NotionPageReader(self.notion_api_token)  # type: ignore[call-arg]

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching documents from Notion")

        # Retrieve all databases and documents the integration can "see"
        self._get_databases()
        documents = self._get_all_documents()

        # Embedding process
        logger.info("Starting embedding process")
        vector_store_index = embed_documents(
            docs=documents,
            embedding_model=self.embedding_model,
        )

        embedded_nodes = map_metadata(
            vector_store_index, include_text=self.include_text
        )

        # Returns a list of document dicts
        # Each document dict has nodeId, embedding, lastRefreshed, metadata
        return embedded_nodes

    def _get_title(self, page: str) -> str:
        headers = {
            "Authorization": f"Bearer {self.notion_api_token}",
            "Notion-Version": f"{self.notion_api_version}",
        }

        try:
            r = requests.get(
                f"{baseURL}/pages/{page}/properties/title", headers=headers, timeout=15
            )
            r.raise_for_status()

            # Extract title
            title = r.json()["results"][0]["title"]["plain_text"]

        except (HTTPError, KeyError) as error:
            logger.warning(f"Failed to get title for page {page}, err: {error}")
            title = ""

        return title

    def _get_databases(self) -> None:
        """
        Returns a list of database IDs.
        """

        headers = {
            "Authorization": f"Bearer {self.notion_api_token}",
            "Notion-Version": f"{self.notion_api_version}",
        }

        payload = {"query": "", "filter": {"value": "database", "property": "object"}}

        try:
            # Send databases query
            r = requests.post(
                f"{baseURL}/search", headers=headers, json=payload, timeout=15
            )

            # Catch errors
            r.raise_for_status()

        except HTTPError as error:
            logger.error(f"Failed to get Notion database IDs, error {error}")
            raise error

        # Load JSON response
        dbs = json.loads(r.content)["results"]

        logger.info(f"Retrieved {len(dbs)} databases")

        self.db_ids = [dbs[i]["id"] for i in range(len(dbs))]

    def _get_all_documents(self) -> Sequence[Document]:
        """
        Returns a list of Documents
        retrieved from a Notion space.

        The Document metadata will contain 'platform':'notion' and
        the Document's URL.

        _get_databases has to have been called previously.
        """

        documents = list()

        # Forcing lastRefreshed to be constant for all documents in this crawler run
        current_time = str(datetime.datetime.utcnow())

        # Add all documents
        for db_id in self.db_ids:
            logger.info(f"Getting documents from db {db_id}")

            try:
                queried = self.NotionReader.load_data(database_ids=[db_id])
            except Exception as e:
                logger.warning(f"Failed to get documents from db {db_id}")
                logger.warning(e)
                continue
            # exclude functionality would be implemented here somewhere
            # regex patterns, url-exclusion, etc.

            # Update queried document metadata with db_id, platform info, link
            for q in queried:
                # Reset page-id, remove hyphens
                q.metadata["pageId"] = q.metadata.pop("page_id").replace("-", "")

                # Add title to metadata
                title = self._get_title(q.metadata["pageId"])
                q.metadata["title"] = title

                # Clean the document text
                q.text = sanitize_text(q.text)

                # Update db_id and platform
                q.metadata["dbId"] = db_id.replace("-", "")  # remove hyphens
                q.metadata["platform"] = "notion"

                # Construct link
                link = f'https://notion.so/{q.metadata["pageId"]}'
                q.metadata["link"] = link

                # Add timestamp (UTC) for crawl-time
                q.metadata["lastRefreshed"] = current_time

            # Extend documents
            logger.info(f"Successfully retrieved {len(queried)} documents")
            documents.extend(queried)

        return documents
