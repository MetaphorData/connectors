import datetime
from typing import Collection, List, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from llama_index.core import Document
from requests.exceptions import HTTPError, RequestException

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import md5_digest
from metaphor.models.crawler_run_metadata import Platform
from metaphor.static_web.config import StaticWebRunConfig
from metaphor.static_web.utils import text_from_HTML, title_from_HTML

logger = get_logger()


class StaticWebExtractor(BaseExtractor):
    """Static webpage extractor."""

    _description = "Crawls webpages and and extracts documents & embeddings."
    _platform = Platform.UNKNOWN

    @staticmethod
    def from_config_file(config_file: str) -> "StaticWebExtractor":
        return StaticWebExtractor(StaticWebRunConfig.from_yaml_file(config_file))

    def __init__(self, config: StaticWebRunConfig):
        super().__init__(config=config)

        self.target_URLs = config.links
        self.target_depths = config.depths

        # Embedding source and configs
        self.embedding_model = config.embedding_model

        self.include_text = config.include_text

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Scraping provided URLs")
        self.documents = list()  # type: List[Document]
        self.visited_pages = set()  # type: set

        for page, depth in zip(self.target_URLs, self.target_depths):
            logger.info(f"Processing {page} with depth {depth}")
            self.current_parent_page = page

            # Fetch target content
            success, content = self._check_page_make_document(page)

            if success:
                logger.info(f"Done with parent page {page}")
                if depth:  # recursive subpage processing
                    await self._process_subpages(page, content, depth)

        # Embedding process
        logger.info("Starting embedding process")

        vector_store_index = embed_documents(
            docs=self.documents,
            embedding_model=self.embedding_model,
        )

        embedded_nodes = map_metadata(
            vector_store_index, include_text=self.include_text
        )

        return embedded_nodes

    async def _process_subpages(
        self,
        parent_URL: str,
        parent_content: str,
        target_depth: int,
        current_depth: int = 1,
    ) -> None:
        logger.info(f"Processing subpages of {parent_URL}")
        subpages = self._get_subpages_from_HTML(parent_content, parent_URL)

        if current_depth > target_depth:  # on recursion depth reached
            return

        for subpage in subpages:
            if subpage in self.visited_pages:
                continue

            logger.info(f"Processing subpage {subpage} of parent {parent_URL}")
            success, content = self._check_page_make_document(subpage)

            if success:
                logger.info(f"Done with subpage {subpage}")
                await self._process_subpages(
                    subpage, content, target_depth, current_depth + 1
                )

    def _check_page_make_document(self, page: str) -> Tuple[bool, str]:
        """
        Gets a page's HTML and adds to the visited pages set.
        If page has valid content, extracts the text and title and generates
        a Document object for the page.

        Returns a bool and the page_content:
            out[0]: False if the page content is invalid, True otherwise.
            out[1]: "" if the page content is invalid, page_content otherwise
        """

        page_content = self._get_page_HTML(page)
        self.visited_pages.add(page)

        if page_content == "ERROR IN PAGE RETRIEVAL":
            return (False, "")
        else:
            page_text = text_from_HTML(page_content)
            page_title = title_from_HTML(page_content)

            page_doc = self._make_document(page, page_title, page_text)
            self.documents.append(page_doc)

            return (True, page_content)

    def _get_page_HTML(self, input_URL: str) -> str:
        """
        Fetches a webpage's content, returning an error message on failure.
        """
        try:
            r = requests.get(input_URL, timeout=5)
            r.raise_for_status()
            return r.text
        except (HTTPError, RequestException) as e:
            logger.warning(f"Error in retrieving {input_URL}, error {e}")
            return "ERROR IN PAGE RETRIEVAL"

    def _get_subpages_from_HTML(self, html_content: str, input_URL: str) -> List[str]:
        """
        Extracts and returns a list of subpage URLs from a given page's HTML and URL.
        Subpage URLs are reconstructed to be absolute URLs and anchor links are trimmed.
        """
        # Retrieve input page

        soup = BeautifulSoup(html_content, "lxml")
        links = soup.find_all("a", href=True)

        # Parse the domain of the input URL
        input_domain = urlparse(self.current_parent_page).netloc
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

    def _make_document(
        self, page_URL: str, page_title: str, page_text: str
    ) -> Document:
        """
        Constructs Document objects from webpage URLs
        and their content, including extra metadata.

        Cleans text content and includes data like page title,
        platform URL, page link, refresh timestamp, and page ID.
        """
        netloc = urlparse(page_URL).netloc
        current_time = str(datetime.datetime.utcnow())

        doc = Document(
            text=sanitize_text(page_text),
            extra_info={
                "title": page_title,
                "platform": netloc,
                "link": page_URL,
                "lastRefreshed": current_time,
                # Create a pageId based on page_URL - is this necessary?
                "pageId": md5_digest(page_URL.encode()),
            },
        )

        return doc
