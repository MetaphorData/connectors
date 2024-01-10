from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.static_web.config import StaticWebRunConfig
from metaphor.static_web.extractor import StaticWebExtractor


@pytest.fixture
def static_web_extractor():
    config = StaticWebRunConfig(
        links=["http://example.com"],
        azure_openAI_key="key",
        azure_openAI_version="version",
        azure_openAI_endpoint="endpoint",
        azure_openAI_model="model",
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
    mock_response = MagicMock()
    mock_response.text = '<html><body><a href="/subpage1">Subpage 1</a></body></html>'
    mock_get.return_value = mock_response

    result = static_web_extractor._get_page_subpages("http://example.com")

    assert "http://example.com" in result
    assert "http://example.com/subpage1" in result
    mock_get.assert_called_once_with("http://example.com", timeout=5)


# test fetch_page_content
@patch("aiohttp.ClientSession.get")
@pytest.mark.asyncio
async def test_fetch_page_content(mock_get, static_web_extractor):
    mock_get.return_value.text = MagicMock(return_value="Page Content")
    mock_get.return_value.raise_for_status = MagicMock()

    content = await static_web_extractor._fetch_page_content(
        static_web_extractor, "http://example.com"
    )

    assert content == "Page Content"
    mock_get.assert_called_once_with("http://example.com", timeout=5)


# test get_text_from_html
def test_get_text_from_html(static_web_extractor):
    html_content = "<html><body><p>Paragraph 1</p><p>Paragraph 2</p></body></html>"
    result = static_web_extractor._get_text_from_html(html_content)

    assert result == "Paragraph 1\nParagraph 2"


# test get_urls_content
@patch("StaticWebExtractor._fetch_page_content")
@pytest.mark.asyncio
async def test_get_urls_content(mock_fetch_page_content, static_web_extractor):
    mock_fetch_page_content.side_effect = ["Content 1", "Content 2"]

    urls = ["http://example.com/page1", "http://example.com/page2"]
    result = await static_web_extractor._get_urls_content(urls)

    assert result == ["Content 1", "Content 2"]


# test make_documents
def test_make_documents(static_web_extractor):
    page_urls = ["http://example.com", "http://example.com/subpage1"]
    page_contents = ["Content 1", "Content 2"]

    result = static_web_extractor._make_documents(page_urls, page_contents)

    assert len(result) == 2
    for doc, url, content in zip(result, page_urls, page_contents):
        assert doc.extra_info["link"] == url
        assert doc.text == content
