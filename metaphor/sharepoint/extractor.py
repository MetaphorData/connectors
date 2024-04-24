import datetime
import json
import traceback
from typing import Collection, Sequence

from bs4 import BeautifulSoup
from bs4.element import Comment
from azure.identity.aio import ClientSecretCredential
from llama_index import Document
from llama_index.readers.microsoft_sharepoint import SharePointReader
from msgraph_beta import GraphServiceClient
from msgraph_beta.generated.sites.sites_request_builder import SitesRequestBuilder
import requests
from requests.exceptions import HTTPError

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.sharepoint.config import SharepointRunConfig

logger = get_logger()

embedding_chunk_size = 512
embedding_overlap_size = 50

class SharepointExtractor(BaseExtractor):
    """Sharepoint site extractor."""
    
    _description = "Extracts Documents from Sharepoint sites & documents."
    _platform = Platform.UNKNOWN

    @staticmethod
    def from_config_file(config_file: str) -> BaseExtractor:
        return SharepointExtractor(SharepointRunConfig.from_yaml_file(config_file))
    
    def __init__(self, config: SharepointRunConfig):
        super().__init__(config=config)  # type: ignore[call-arg]

        # Authorization configs
        self.client_id = config.client_id
        self.client_secret = config.client_seceret
        self.tenant_id = config.tenant_id

        # Initialize SharePointReader
        # TODO

async def extract(self) -> Collection[ENTITY_TYPES]:
    logger.info(f"Getting documents from Sharepoint instance.")

    pass

def _get_SiteCollections():
    pass

def _get_Sites():
    pass

def _get_SitePages():
    pass

def _get_webParts():
    pass