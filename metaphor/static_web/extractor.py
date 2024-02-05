import datetime
from typing import Collection, List
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Comment
from llama_index import Document
from requests.exceptions import HTTPError, RequestException

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.logger import get_logger
from metaphor.common.utils import md5_digest
from metaphor.models.crawler_run_metadata import Platform
from metaphor.static_web.config import StaticWebRunConfig

logger = get_logger()

embedding_chunk_size = 512
embedding_overlap_size = 50


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

        self.azure_openAI_key = config.azure_openAI_key
        self.azure_openAI_version = config.azure_openAI_version
        self.azure_openAI_endpoint = config.azure_openAI_endpoint
        self.azure_openAI_model = config.azure_openAI_model
        self.azure_openAI_model_name = config.azure_openAI_model_name

        self.include_text = config.include_text

    async def extract(self) -> Collection[dict]:
        logger.info("Scraping provided URLs")
        self.docs = []
        self.visited_pages = set()

        for page, depth in zip(self.target_URLs, self.target_depths):
            logger.info(f"Processing {page} with depth {depth}")
            self.current_parent_page = page

            # Fetch target content
            page_content = self._get_page_HTML(page)
            self.visited_pages.add(page)

            if page_content != "ERROR IN PAGE RETRIEVAL":  # proceed if successful
                main_text = self._get_text_from_HTML(page_content)
                main_title = self._get_title_from_HTML(page_content)

                doc = self._make_document(page, main_title, main_text)
                self.docs.append(doc)

                logger.info(f"Done with parent page {page}")

                if depth:  # recursive subpage processing
                    await self._process_subpages(page, main_title, depth)

        # Embedding process
        logger.info("Starting embedding process")
        vsi = embed_documents(
            self.docs,
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

    async def _process_subpages(
        self, parent_URL: str, parent_content: str, depth: int, current_depth: int = 1
    ) -> None:
        logger.info(f"Processing subpages of {parent_URL}")
        subpages = self._get_subpages_from_HTML(parent_content, parent_URL)

        for subpage in subpages:
            if subpage in self.visited_pages:
                continue

            logger.info(f"Processing subpage {subpage}")
            subpage_content = self._get_page_HTML(subpage)
            self.visited_pages.add(subpage)

            if subpage_content == "ERROR IN PAGE RETRIEVAL":
                continue

            subpage_text = self._get_text_from_HTML(subpage_content)
            subpage_title = self._get_title_from_HTML(subpage_content)

            subpage_doc = self._make_document(subpage, subpage_title, subpage_text)

            self.docs.append(subpage_doc)

            if current_depth < depth:
                await self._process_subpages(
                    subpage, subpage_content, depth, current_depth + 1
                )

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

    def _get_text_from_HTML(self, html_content: str) -> str:
        """
        Extracts and returns visible text from given HTML content as a single string.
        Designed to handle output from get_page_HTML.
        """

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
        Designed to handle output from get_page_HTML.
        """

        soup = BeautifulSoup(html_content, "lxml")
        title_tag = soup.find("title")

        if title_tag:
            return title_tag.text

        else:
            return ""

    def _make_document(
        self, page_URL: str, page_title: str, page_text: str
    ) -> Document:
        """
        Constructs Document objects from webpage URLs and their content, including extra metadata.
        Cleans text content and includes data like page title, platform URL, page link, refresh timestamp, and page ID.
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
