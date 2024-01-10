import asyncio
import datetime
from typing import Sequence

import aiohttp
import requests
from aiohttp.client_exceptions import ClientResponseError
from llama_index import Document
from lxml import etree
from requests.exceptions import HTTPError

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.logger import get_logger
from metaphor.static_web.config import StaticWebRunConfig

logger = get_logger()

embedding_chunk_size = 512
embedding_overlap_size = 50


class StaticWebExtractor(BaseExtractor):
    """Static webpage extractor."""

    @staticmethod
    def from_config_file(config_file: str) -> "StaticWebExtractor":
        return StaticWebExtractor(StaticWebRunConfig.from_yaml_file(config_file))

    def __init__(self, config: StaticWebRunConfig):
        super().__init__(config=config)

        self.target_URLs = config.links

        self.azure_openAI_key = config.azure_openAI_key
        self.azure_openAI_version = config.azure_openAI_version
        self.azure_openAI_endpoint = config.azure_openAI_endpoint
        self.azure_openAI_model = config.azure_openAI_model
        self.azure_openAI_model_name = config.azure_openAI_model_name

        self.include_text = config.include_text

    async def extract(self) -> Sequence[dict]:
        logger.info("Scraping provided URLs")

        docs = []

        # Retrieval for all pages
        for page in self.target_URLs:
            logger.info(f"Processing {page}")

            subpages = self._get_page_subpages(page)
            page_contents = await self._get_urls_content(subpages)

            p_docs = self._make_documents(subpages, page_contents)

            docs.extend(p_docs)

        # Embedding process
        logger.info("Starting embedding process")
        vsi = embed_documents(
            docs,
            self.azure_openAI_key,
            self.azure_openAI_version,
            self.azure_openAI_endpoint,
            self.azure_openAI_model,
            self.azure_openAI_model_name,
            embedding_chunk_size,
            embedding_overlap_size,
        )

        embedded_nodes = map_metadata(vsi, include_text=self.include_text)

        return embedded_nodes

    def _get_page_subpages(self, input_URL: str) -> Sequence[str]:
        """
        Extracts and returns a list of subpage URLs from a given webpage URL.
        The list includes the original URL followed by URLs of all found subpages.
        Subpage URLs are reconstructed to be absolute URLs.
        """
        if input_URL.endswith("/"):
            input_URL = input_URL[:-1]
        pages = [input_URL]
        try:
            r = requests.get(input_URL, timeout=5)
            r.raise_for_status()
        except HTTPError:
            logger.warn(f"Couldn't retrieve page {input_URL}")
            return pages

        html = etree.HTML(r.text)

        base_URL = "/".join(input_URL.split("/")[0:3])

        pages.extend(
            [base_URL + a for a in html.xpath(".//a/@href") if a.startswith("/")]
        )

        return pages

    # exponential backoff? or is it ok if things fail
    async def _fetch_page_content(
        self, session: aiohttp.ClientSession, input_URL: str
    ) -> str:
        """
        Asynchronously fetches a webpage's content, returning an error message on failure.
        Args: aiohttp.ClientSession and the webpage URL.
        """
        try:
            async with session.get(
                input_URL, timeout=5
            ) as response:  # Setting a 5-second timeout
                response.raise_for_status()
                return await response.text()
        except ClientResponseError:
            logger.warn(f"Error in retrieving {input_URL}")
            return "ERROR IN PAGE RETRIEVAL"  # Error handling

    def _get_text_from_html(self, html_content: str) -> str:
        """
        Extracts and returns text from given HTML content as a single string.
        Designed to handle output from fetch_page_content.
        """
        if html_content == "ERROR IN PAGE RETRIEVAL":
            return html_content  # Return the error message as is

        tree = etree.HTML(html_content)
        return "\n".join([element.text for element in tree.iter() if element.text])

    async def _get_urls_content(self, urls: Sequence[str]) -> Sequence[str]:
        """
        Asynchronously fetches and processes content from a list of URLs.
        Returns a list of text content or error messages for each URL.
        """
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_page_content(session, url) for url in urls]
            pages_content = await asyncio.gather(*tasks)
            return [self._get_text_from_html(content) for content in pages_content]

    def _make_documents(self, page_URLs, page_contents):
        """
        Constructs Document objects from webpage URLs and their content, including extra metadata.
        Cleans text content and includes data like platform URL, page link, refresh timestamp, and page ID.
        """
        docs = []
        base_URL = "/".join(page_URLs[0].split("/")[0:3])
        current_time = str(datetime.datetime.utcnow())
        for url, content in zip(page_URLs, page_contents):
            # Skip pages that weren't retrieved successfully
            if "ERROR IN PAGE RETRIEVAL" in content:
                continue

            doc = Document(
                text=sanitize_text(content),
                extra_info={
                    "platform": base_URL,
                    "link": url,
                    "lastRefreshed": current_time,
                    "pageId": hash(
                        url
                    ),  # Create a pageId based on URL - is this necessary?
                },
            )
            docs.append(doc)

        return docs
