import asyncio
import datetime
from typing import Collection, List, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
import requests
from aiohttp.client_exceptions import ClientResponseError
from bs4 import BeautifulSoup
from bs4.element import Comment
from llama_index import Document
from requests.exceptions import HTTPError, RequestException

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.logger import get_logger
from metaphor.common.utils import md5_digest, unique_list
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
        self.target_depths = config.depths

        self.azure_openAI_key = config.azure_openAI_key
        self.azure_openAI_version = config.azure_openAI_version
        self.azure_openAI_endpoint = config.azure_openAI_endpoint
        self.azure_openAI_model = config.azure_openAI_model
        self.azure_openAI_model_name = config.azure_openAI_model_name

        self.include_text = config.include_text

    async def extract(self) -> Collection[dict]:
        logger.info("Scraping provided URLs")
        docs = []

        # Retrieval for all pages
        for page, depth in zip(self.target_URLs, self.target_depths):
            logger.info(f"Processing {page}")

            if depth:
                self._current_parent_page = page
                subpages = self._get_page_subpages(page)
                for _ in range(1, depth):
                    for subpage in subpages:
                        logger.info(f"Processing subpage {subpage}")
                        subpages.extend(self._get_page_subpages(subpage))
                        subpages = unique_list(subpages)
            else:
                subpages = []
                subpages.append(page)

            page_titles, page_contents = await self._get_URLs_HTML(subpages)

            p_docs = self._make_documents(subpages, page_titles, page_contents)

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

    def _get_page_subpages(self, input_URL: str) -> List[str]:
        """
        Extracts and returns a list of subpage URLs from a given webpage URL.
        Subpage URLs are reconstructed to be absolute URLs and anchor links are trimmed.
        """
        # Retrieve input page
        try:
            response = requests.get(input_URL, timeout=5)
            response.raise_for_status()
        except (HTTPError, RequestException) as e:
            logger.warning(f"Error in {input_URL} retrieval, error: {e}")
            return [input_URL]

        soup = BeautifulSoup(response.text, "lxml")
        links = soup.find_all("a", href=True)

        # Parse the domain of the input URL
        input_domain = urlparse(self._current_parent_page).netloc
        subpages = [input_URL]

        # Find eligible links
        for link in links:
            href = link["href"]
            full_url = urljoin(input_URL, href)

            # Check if the domain of the full URL matches the input domain
            if urlparse(full_url).netloc == input_domain:
                # Remove any query parameters or fragments
                full_url = urljoin(full_url, urlparse(full_url).path)
                if full_url not in subpages:
                    subpages.append(full_url)

        return subpages

    # exponential backoff? or is it ok if things fail
    async def _fetch_page_HTML(
        self, session: aiohttp.ClientSession, input_URL: str
    ) -> str:
        """
        Asynchronously fetches a webpage's content, returning an error message on failure.
        Args: aiohttp.ClientSession and the webpage URL.
        """
        try:
            async with session.get(input_URL, timeout=5) as response:
                response.raise_for_status()
                return await response.text()
        except ClientResponseError:
            logger.warning(f"Error in retrieving {input_URL}")
            return "ERROR IN PAGE RETRIEVAL"  # Error handling

    def _get_text_from_HTML(self, html_content: str) -> str:
        """
        Extracts and returns visible text from given HTML content as a single string.
        Designed to handle output from fetch_page_content.
        """
        if html_content == "ERROR IN PAGE RETRIEVAL":
            return html_content  # Return the error message as is

        def filter_visible(el):
            if el.parent.name in [
                "style",
                "script",
                "head",
                "title",
                "meta",
                "[document]",
            ]:
                return False
            elif isinstance(el, Comment):
                return False
            else:
                return True

        # Use bs4 to find visible text elements
        soup = BeautifulSoup(html_content, "lxml")
        visible_text = filter(filter_visible, soup.findAll(string=True))
        return "\n".join(t.strip() for t in visible_text)

    def _get_title_from_HTML(self, html_content: str) -> str:
        """
        Extracts the title of a webpage given HTML content as a single string.
        Designed to handle output from fetch_page_content.
        """

        if html_content == "ERROR IN PAGE RETRIEVAL":
            return ""  # Return no title

        soup = BeautifulSoup(html_content, "lxml")
        title_tag = soup.find("title")

        if title_tag:
            return title_tag.text

        else:
            return ""

    async def _get_URLs_HTML(self, urls: List[str]) -> Tuple[List[str], List[str]]:
        """
        Asynchronously fetches and processes content from a list of URLs.
        Returns a list of text content or error messages for each URL.
        """
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_page_HTML(session, url) for url in urls]
            pages_content = await asyncio.gather(*tasks)
            page_texts = [
                self._get_text_from_HTML(content) for content in pages_content
            ]
            page_titles = [
                self._get_title_from_HTML(content) for content in pages_content
            ]

            return page_titles, page_texts

    def _make_documents(
        self, page_URLs: List[str], page_titles: List[str], page_contents: List[str]
    ):
        """
        Constructs Document objects from webpage URLs and their content, including extra metadata.
        Cleans text content and includes data like page title, platform URL, page link, refresh timestamp, and page ID.
        """
        docs = []
        base_URL = urlparse(page_URLs[0]).netloc
        current_time = str(datetime.datetime.utcnow())
        for url, title, content in zip(page_URLs, page_titles, page_contents):
            # Skip pages that weren't retrieved successfully
            if "ERROR IN PAGE RETRIEVAL" in content:
                continue

            doc = Document(
                text=sanitize_text(content),
                extra_info={
                    "title": title,
                    "platform": base_URL,
                    "link": url,
                    "lastRefreshed": current_time,
                    # Create a pageId based on URL - is this necessary?
                    "pageId": md5_digest(url.encode()),
                },
            )
            docs.append(doc)

        return docs
