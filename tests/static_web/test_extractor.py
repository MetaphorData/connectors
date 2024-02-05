from unittest.mock import MagicMock, patch

import pytest
import requests

from metaphor.common.base_config import OutputConfig
from metaphor.static_web.config import StaticWebRunConfig
from metaphor.static_web.extractor import StaticWebExtractor
from tests.test_utils import load_json


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


# Test for successful page HTML retrieval
@patch("requests.get")
@pytest.mark.asyncio
def test_get_page_HTML_success(mock_get, static_web_extractor):
    mock_get_val = MagicMock()
    mock_get_val.text = "<html><body>Test</body></html>"

    mock_get.return_value = mock_get_val

    content = static_web_extractor._get_page_HTML("https://example.com")
    assert content == "<html><body>Test</body></html>"


# Test for handling retrieval failure
@patch("requests.get")
@pytest.mark.asyncio
def test_get_page_HTML_failure(mock_get, static_web_extractor):
    mock_get_val = MagicMock()

    mock_get.side_effect = requests.RequestException()
    mock_get.return_value = mock_get_val

    content = static_web_extractor._get_page_HTML("https://example.com")
    assert content == "ERROR IN PAGE RETRIEVAL"


# Test for extracting subpages from HTML
def test_get_subpages_from_HTML(static_web_extractor):
    html_content = '<html><body><a href="/test">Link</a></body></html>'
    static_web_extractor.current_parent_page = "https://example.com"
    result = static_web_extractor._get_subpages_from_HTML(
        html_content, "https://example.com"
    )
    assert "https://example.com/test" in result


# Test for extracting visible text from HTML, with filtering
def test_get_text_from_HTML_with_filtering(static_web_extractor):
    html_content = """
    <html>
        <head>
            <title>Test Title</title>
            <style>Some style</style>
            <script>Some script</script>
            <meta name="description" content="Some meta">
        </head>
        <body>
            <p>Visible paragraph 1.</p>
            <div>
                <p>Visible paragraph 2.</p>
                <!-- Commented text -->
                <script>Script text</script>
                <style>Style text</style>
            </div>
        </body>
    </html>
    """
    text = static_web_extractor._get_text_from_HTML(html_content)
    assert "Visible paragraph 1." in text
    assert "Visible paragraph 2." in text
    assert "Test Title" not in text
    assert "Some style" not in text
    assert "Some script" not in text
    assert "Some meta" not in text
    assert "Commented text" not in text
    assert "Script text" not in text
    assert "Style text" not in text


# Test for extracting title from HTML
def test_get_title_from_HTML_success(static_web_extractor):
    html_content = "<html><head><title>Test Title</title></head></html>"
    title = static_web_extractor._get_title_from_HTML(html_content)
    assert title == "Test Title"


# Test for extracting empty title
def test_get_title_from_HTML_failure(static_web_extractor):
    html_content = "<html><head></head><body><h1>Hello World!</h1></body></html>"
    title = static_web_extractor._get_title_from_HTML(html_content)
    assert title == ""


# Test for making a document
def test_make_document(static_web_extractor):
    doc = static_web_extractor._make_document(
        "https://example.com", "Test Title", "Test Content"
    )
    assert doc.text == "Test Content"
    assert doc.extra_info["title"] == "Test Title"
    assert doc.extra_info["link"] == "https://example.com"


# Test extract
@patch("metaphor.static_web.StaticWebExtractor._get_page_HTML")
@patch("metaphor.static_web.StaticWebExtractor._process_subpages")
@patch("metaphor.static_web.extractor.embed_documents")
@pytest.mark.asyncio
async def test_extractor(
    mock_embed_docs: MagicMock,
    mock_process_subpages: MagicMock,
    mock_get_HTML: MagicMock,
    static_web_extractor,
    test_root_dir: str,
):
    # mock VSI
    mock_VSI = MagicMock()

    mock_VSI.storage_context.to_dict.return_value = {
        "vector_store": {
            "default": {
                "embedding_dict": {"abcd1234": [0.1, 0.2, 0.3, 0.4]},
                "metadata_dict": {
                    "abcd1234": {
                        "title": "Hello World!",
                        "pageId": "e19d5cd5af0378da05f63f891c7467af",
                        "platform": "example.com",
                        "link": "https://example.com",
                        "lastRefreshed": "2024-02-05 00:00:00.000000",
                    }
                },
            }
        },
        "doc_store": {
            "docstore/data": {"abcd1234": {"__data__": {"text": "Hello World!"}}}
        },
    }

    mock_embed_docs.return_value = mock_VSI

    mock_process_subpages.return_value = None

    mock_get_HTML.return_value = "<html><head></head><body><h1>Hello World!</h1><a href='/test'>Link</a></body></html>"

    events = await static_web_extractor.extract()

    assert events == load_json(f"{test_root_dir}/static_web/expected.json")
