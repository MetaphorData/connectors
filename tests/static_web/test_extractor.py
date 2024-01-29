from unittest.mock import MagicMock, patch

import aiohttp
import pytest
import requests

from metaphor.common.base_config import OutputConfig
from metaphor.static_web.config import StaticWebRunConfig
from metaphor.static_web.extractor import StaticWebExtractor


@pytest.fixture
def static_web_extractor():
    config = StaticWebRunConfig(
        links=["https://example.com"],
        depths=[1],
        azure_openAI_key="key",
        azure_openAI_version="version",
        azure_openAI_endpoint="endpoint",
        azure_openAI_model="text-embedding-ada-002",
        azure_openAI_model_name="model_name",
        include_text=True,
        output=OutputConfig(),
    )
    return StaticWebExtractor(config)


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return


# test get_page_subpages
@patch("requests.get")
def test_get_page_subpages(mock_get, static_web_extractor):
    # Successful retrieval case
    mock_response = MagicMock()
    mock_response.text = '<html><body><a href="/subpage1">Subpage 1</a></body></html>'
    mock_get.return_value = mock_response
    static_web_extractor._current_parent_page = "https://example.com"
    result = static_web_extractor._get_page_subpages("https://example.com")
    assert "https://example.com" == result[0]
    assert "https://example.com/subpage1" == result[1]

    # Exception handling case
    mock_get.side_effect = requests.exceptions.RequestException()
    result = static_web_extractor._get_page_subpages("https://example.com")
    assert result == ["https://example.com"]


# test fetch_page_HTML
@patch("aiohttp.ClientSession")
@pytest.mark.asyncio
async def test_fetch_page_HTML(mock_session, static_web_extractor):
    # Successful retrieval case
    mock_session.get.return_value.__aenter__.return_value.text.return_value = (
        "Page Content"
    )
    content = await static_web_extractor._fetch_page_HTML(
        mock_session, "https://example.com"
    )
    assert content == "Page Content"

    # Exception handling case
    mock_session.get.side_effect = aiohttp.ClientResponseError(None, None, status=500)
    content = await static_web_extractor._fetch_page_HTML(
        mock_session, "https://example.com"
    )
    assert content == "ERROR IN PAGE RETRIEVAL"


# test get_text_from_HTML
def test_get_text_from_HTML(static_web_extractor):
    # Normal content
    html_content = "<html><body><p>Paragraph 1</p><p>Paragraph 2</p></body></html>"
    result = static_web_extractor._get_text_from_HTML(html_content)
    assert result == "Paragraph 1\nParagraph 2"

    # Error message content
    error_content = "ERROR IN PAGE RETRIEVAL"
    result = static_web_extractor._get_text_from_HTML(error_content)
    assert result == error_content


# test get_URLs_HTML
@patch("metaphor.static_web.StaticWebExtractor._fetch_page_HTML")
@pytest.mark.asyncio
async def test_get_URLs_HTML(mock_fetch, static_web_extractor):
    # Mocking _fetch_page_HTML to return HTML content
    mock_fetch.return_value = (
        "<html><head><title>Title 1</title></head><body>Content 1</body></html>"
    )
    # Test
    page_titles, page_contents = await static_web_extractor._get_URLs_HTML(
        ["https://example.com"]
    )
    # Assertions
    assert page_titles == ["Title 1"]
    assert page_contents == ["Content 1"]

    # Mocking _fetch_page_HTML to return error
    mock_fetch.return_value = "ERROR IN PAGE RETRIEVAL"
    # Test
    page_titles, page_contents = await static_web_extractor._get_URLs_HTML(
        ["https://example.com"]
    )
    # Assertions
    assert page_titles == [""]
    assert page_contents == ["ERROR IN PAGE RETRIEVAL"]


# test make_documents
def test_make_documents(static_web_extractor):
    page_urls = ["https://example.com", "https://example.com/subpage1"]
    page_titles = ["Title 1", "Title 2"]
    page_contents = ["Content 1", "ERROR IN PAGE RETRIEVAL"]

    result = static_web_extractor._make_documents(page_urls, page_titles, page_contents)

    assert len(result) == 1
    doc = result[0]
    assert doc.extra_info["link"] == "https://example.com"
    assert doc.extra_info["title"] == "Title 1"
    assert doc.text == "Content 1"
