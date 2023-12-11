import datetime
import json
import traceback
from typing import Collection

import requests
from llama_index import Document, download_loader
from requests.exceptions import HTTPError

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.notion.config import NotionRunConfig

logger = get_logger()

baseurl = "https://api.notion.com/v1/"


class NotionExtractor(BaseExtractor):
    """Notion Document extractor."""

    @staticmethod
    def from_config_file(config_file: str) -> "NotionExtractor":
        return NotionExtractor(NotionRunConfig.from_yaml_file(config_file))

    def __init__(self, config: NotionRunConfig):
        super().__init__(config, "Notion document crawler", Platform.UNKNOWN)

        self.notion_api_tok = config.notion_api_tok
        self.notion_api_version = config.notion_api_version

        self.azure_openAI_key = config.azure_openAI_key
        self.azure_openAI_ver = config.azure_openAI_ver
        self.azure_openAI_endpoint = config.azure_openAI_endpoint
        self.azure_openAI_model = config.azure_openAI_model
        self.azure_openAI_model_name = config.azure_openAI_model_name

        self.include_text = config.include_text
        self.embedding_chunk_size = 512
        self.embedding_overlap_size = 50

        # Set up LlamaIndex Notion integration
        npr = download_loader("NotionPageReader")
        self.NotionReader = npr(integration_token=self.notion_api_tok)

    async def extract(self) -> Collection[dict]:
        logger.info("Fetching documents from Notion")

        # this method should do all the things
        self._get_databases()
        docs = self._get_all_documents()

        # call embedder function here
        logger.info("Starting embedding process")
        VSI = embed_documents(
            docs,
            self.azure_openAI_key,
            self.azure_openAI_ver,
            self.azure_openAI_endpoint,
            self.azure_openAI_model,
            self.azure_openAI_model_name,
            self.embedding_chunk_size,
            self.embedding_overlap_size,
        )

        # get vector_store from VectorStoreIndex
        vector_store = VSI.storage_context.to_dict()["vector_store"]["default"]
        doc_store = VSI.storage_context.to_dict()["doc_store"]

        # map metadata back to each node
        embedded_nodes = map_metadata(
            embedding_dict=vector_store["embedding_dict"],
            metadata_dict=vector_store["metadata_dict"],
            include_text=self.include_text,
            doc_store=doc_store["docstore/data"],
        )

        # currently returns a list of document dicts
        # each document dict has nodeId, embedding, lastRefreshed, metadata
        return embedded_nodes

    def _get_databases(self) -> Collection[str]:
        """
        Returns a list of database IDs.
        """

        headers = {
            "Authorization": f"Bearer {self.notion_api_tok}",
            "Notion-Version": f"{self.notion_api_version}",
        }

        payload = {"query": "", "filter": {"value": "database", "property": "object"}}

        try:
            # send request
            r = requests.post(
                baseurl + "search", headers=headers, json=payload, timeout=15
            )

            # throw error if 400
            r.raise_for_status()

            # load json
            dbs = json.loads(r.content)["results"]

            logger.info(f"Retrieved {len(dbs)} databases")

        except HTTPError as error:
            traceback.print_exc()
            logger.error(f"Failed to get Notion database IDs, error {error}")

        self.db_ids = [dbs[i]["id"] for i in range(len(dbs))]

        return self.db_ids

    def _get_all_documents(self) -> Collection[Document]:
        """
        Returns a list of Documents
        retrieved from a Notion space.

        The Document metadata will contain 'platform':'notion' and
        the Document's URL.

        _get_databases has to have been called previously.
        """

        self.docs = list()

        # forcing lastRefreshed to be constant for all documents in this crawler run
        current_time = str(datetime.datetime.utcnow())

        # add all documents
        for db_id in self.db_ids:
            logger.info(f"Getting documents from db {db_id}")

            try:
                queried = self.NotionReader.load_data(database_id=db_id)

            except Exception:
                logger.warning(f"Failed to get documents from db {db_id}")

            # exclude functionality would be implemented here somewhere
            # regex patterns, url-exclusion, etc.

            # update queried document metadata with db_id, platform info, link

            for q in queried:
                # update db_id and platform
                q.metadata["dbId"] = db_id.replace("-", "")  # remove hyphens
                q.metadata["platform"] = "notion"

                # reset page-id, remove hyphens
                q.metadata["pageId"] = q.metadata.pop("page_id").replace("-", "")

                # construct link
                link = f'https://notion.so/{q.metadata["pageId"]}'
                q.metadata["link"] = link

                # add timestamp (UTC) for crawl-time
                q.metadata["lastRefreshed"] = current_time

            # extend documents
            logger.info(f"Successfully retrieved {len(queried)} documents")
            self.docs.extend(queried)

        return self.docs
