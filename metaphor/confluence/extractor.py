import datetime
import json
from typing import Collection

import requests
from llama_index.core import Document
from requests.exceptions import HTTPError, RequestException

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.confluence.config import ConfluenceRunConfig

logger = get_logger()

embedding_chunk_size = 512
embedding_overlap_size = 50

class ConfluenceExtractor(BaseExtractor):
    """Confluence Page Extractor."""

    _description = "Extracts pages from a Confluence instance."
    _platform = Platform.UNKNOWN

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

        # Azure OpenAI
        self.azure_openAI_key = config.azure_openAI_key
        self.azure_openAI_version = config.azure_openAI_version
        self.azure_openAI_endpoint = config.azure_openAI_endpoint
        self.azure_openAI_model = config.azure_openAI_model
        self.azure_openAI_model_name = config.azure_openAI_model_name

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching pages from Confluence: {self.confluence_base_URL}")
        self._check_configs()
        pass

    def _check_configs(self) -> None:
        # check that only one of space_key, page_ids, label, cql are present
        # check that the one that is present matches select_method
        # check that only one authentication method is present
        # check that authentication method matches the confluence_cloud option

        # check that the baseURL does not end in "/", fix
        pass