from datetime import datetime, timezone
from typing import Collection, Dict, Tuple

from azure.identity.aio import ClientSecretCredential
from llama_index.core import Document
from msgraph_beta import GraphServiceClient
from msgraph_beta.generated.sites.sites_request_builder import SitesRequestBuilder

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.sharepoint.config import SharepointRunConfig
from metaphor.static_web.utils import text_from_HTML

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
        self.sharepoint_client_id = config.sharepoint_client_id
        self.sharepoint_client_secret = config.sharepoint_client_secret
        self.sharepoint_tenant_id = config.sharepoint_tenant_id

        # Azure OpenAI
        self.azure_openAI_key = config.azure_openAI_key
        self.azure_openAI_version = config.azure_openAI_version
        self.azure_openAI_endpoint = config.azure_openAI_endpoint
        self.azure_openAI_model = config.azure_openAI_model
        self.azure_openAI_model_name = config.azure_openAI_model_name

        # include_text
        self.include_text = config.include_text

        self.ClientSecretCredential = ClientSecretCredential(
            tenant_id=self.sharepoint_tenant_id,
            client_id=self.sharepoint_client_id,
            client_secret=self.sharepoint_client_secret,
        )

        self.GraphServiceClient = GraphServiceClient(
            credentials=self.ClientSecretCredential
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Getting documents from Sharepoint instance.")
        documents = []

        sites = await self._get_sites()

        for site_name, site_id in sites.items():
            logger.info(f"Processing site {site_name}")
            pages = await self._get_site_pages(site_id)

            for page_title, page_id, page_URL in pages:
                current_time = str(datetime.now(timezone.utc))
                logger.info(f"\tProcessing page {page_title}")
                page_body = await self._get_webParts_HTML(site_id, page_id)
                documents.append(
                    Document(
                        text=sanitize_text(page_body),
                        extra_info={
                            "title": page_title,
                            "platform": f"sharepoint/{site_name}",
                            "link": page_URL,
                            "lastRefreshed": current_time,
                            "pageId": page_id,
                        },
                    )
                )

        logger.info("Starting embedding process")

        vector_store_index = embed_documents(
            documents,
            self.azure_openAI_key,
            self.azure_openAI_version,
            self.azure_openAI_endpoint,
            self.azure_openAI_model,
            self.azure_openAI_model_name,
            embedding_chunk_size,
            embedding_overlap_size,
        )

        embedded_nodes = map_metadata(
            vector_store_index, include_text=self.include_text
        )

        return embedded_nodes

    async def _get_site_collections(self):
        """
        Gets root siteCollection.
        TODO: check if necessary
        """
        # not sure if this is necessary
        # depends on what a client's sharepoint setup is
        params = SitesRequestBuilder.SitesRequestBuilderGetQueryParameters(
            select=["siteCollection", "webUrl", "name", "id"],
            filter="siteCollection/root ne null",
        )
        config = SitesRequestBuilder.SitesRequestBuilderGetRequestConfiguration(
            query_parameters=params
        )

        results = await self.GraphServiceClient.sites.get(request_configuration=config)

        return results

    async def _get_sites(self) -> Dict[str, str]:
        """
        Returns a dictionary of form (site_name, site_id) for all Sharepoint Sites
            that the integration has access to.
        """
        params = SitesRequestBuilder.SitesRequestBuilderGetQueryParameters(
            select=["isPersonalSite", "webUrl", "name", "id"],
            filter="not isPersonalSite and name ne null",
        )
        config = SitesRequestBuilder.SitesRequestBuilderGetRequestConfiguration(
            query_parameters=params
        )

        all_sites = await self.GraphServiceClient.sites.get_all_sites.get(
            request_configuration=config
        )

        sites = {site.name.replace(" ", ""): site.id for site in all_sites.value}

        return sites

    async def _get_site_pages(self, site_id: str) -> Collection[Tuple[str, str, str]]:
        """
        Returns a list of pages from given site_id
            of form (page_title, page_id, web_url).
        """

        result = await self.GraphServiceClient.sites.by_site_id(
            site_id
        ).pages.graph_site_page.get()

        pages = [(site.title, site.id, site.web_url) for site in result.value]

        return pages

    async def _get_webParts_HTML(self, site_id: str, page_id: str) -> str:
        """
        Gets webParts from given site_id and page_id.

        Returns all HTML elements as a single string.
        """

        result = (
            await self.GraphServiceClient.sites.by_site_id(site_id)
            .pages.by_base_site_page_id(page_id)
            .graph_site_page.web_parts.get()
        )

        webParts_html = "\n".join(
            [x.inner_html for x in result.value if hasattr(x, "inner_html")]
        )

        return text_from_HTML(webParts_html)
