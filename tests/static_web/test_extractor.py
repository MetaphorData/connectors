from unittest.mock import MagicMock, patch

import pytest
import requests

from metaphor.common.base_config import OutputConfig
from metaphor.static_web.config import StaticWebRunConfig
from metaphor.static_web.extractor import StaticWebExtractor
from tests.test_utils import load_json, load_text


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
def test_get_page_HTML_success(mock_get: MagicMock, static_web_extractor):
    mock_get_val = MagicMock()
    mock_get_val.text = "<html><body>Test</body></html>"

    mock_get.return_value = mock_get_val

    content = static_web_extractor._get_page_HTML("https://example.com")
    assert content == "<html><body>Test</body></html>"


# Test for handling retrieval failure
@patch("requests.get")
def test_get_page_HTML_failure(mock_get: MagicMock, static_web_extractor):
    mock_get_val = MagicMock()

    mock_get.side_effect = requests.RequestException()
    mock_get.return_value = mock_get_val

    content = static_web_extractor._get_page_HTML("https://example.com")
    assert content == "ERROR IN PAGE RETRIEVAL"


# Test for extracting subpages from HTML
def test_get_subpages_from_HTML(static_web_extractor):
    html_content = "<html><body><a href='/test'>Link</a></body></html>"
    static_web_extractor.current_parent_page = "https://example.com"
    result = static_web_extractor._get_subpages_from_HTML(
        html_content, "https://example.com"
    )
    assert "https://example.com/test" in result


# Test for making a document
def test_make_document(static_web_extractor):
    doc = static_web_extractor._make_document(
        "https://example.com", "Test Title", "Test Content"
    )
    assert doc.text == "Test Content"
    assert doc.extra_info["title"] == "Test Title"
    assert doc.extra_info["link"] == "https://example.com"


# Test process subpages
@patch("metaphor.static_web.StaticWebExtractor._get_page_HTML")
@patch("metaphor.static_web.StaticWebExtractor._get_subpages_from_HTML")
@pytest.mark.asyncio
async def test_process_subpages(
    mock_get_subpages_from_HTML: MagicMock,
    mock_get_page_HTML: MagicMock,
    static_web_extractor,
):
    # Mocking the responses for page HTML and subpages
    parent_html = "<html><body><a href='/subpage1'>Subpage 1</a></body></html>"
    subpage1_html = "<html><body><p>Content of Subpage 1</p></body></html>"

    mock_get_page_HTML.side_effect = [parent_html, subpage1_html]
    mock_get_subpages_from_HTML.return_value = ["https://example.com/subpage1"]

    # Call the _process_subpages method
    static_web_extractor.visited_pages = set()
    static_web_extractor.documents = list()
    await static_web_extractor._process_subpages("https://example.com", parent_html, 2)

    # Check if _get_page_HTML is called for subpages
    mock_get_page_HTML.assert_any_call("https://example.com/subpage1")

    # Verify that documents are added correctly
    assert len(static_web_extractor.documents) > 0
    assert (
        static_web_extractor.documents[0].extra_info["link"]
        == "https://example.com/subpage1"
    )
    # Further assertions can be made based on the structure of your Document objects

    # Check if the depth limit is respected
    assert not any(
        "subpage2" in doc.extra_info["link"] for doc in static_web_extractor.documents
    )


# Test 1 layer recursion
@patch("metaphor.static_web.StaticWebExtractor._get_page_HTML")
@patch("metaphor.static_web.extractor.embed_documents")
@patch("metaphor.static_web.extractor.map_metadata")
@pytest.mark.asyncio
async def test_shallow_recursion(
    mock_map_metadata: MagicMock,
    mock_embed_docs: MagicMock,
    mock_get_HTML: MagicMock,
    static_web_extractor,
    test_root_dir: str,
):
    mock_map_metadata.return_value = []
    mock_embed_docs.return_value = []

    # Mock pages appropriately
    page_folder = f"{test_root_dir}/static_web/sample_pages"

    mock_get_HTML.side_effect = [
        load_text(f"{page_folder}/main.html"),
        load_text(f"{page_folder}/page1.html"),
        load_text(f"{page_folder}/page2.html"),
        load_text(f"{page_folder}/page3.html"),
        load_text(f"{page_folder}/page4.html"),
    ]

    # Initialize extractor attributes for shallow recursion test
    static_web_extractor.target_URLs = ["https://example.com/main"]
    static_web_extractor.target_depths = [1]

    await static_web_extractor.extract()

    assert len(static_web_extractor.visited_pages) == 3
    assert len(static_web_extractor.documents) == 3


# Test infinite
@patch("metaphor.static_web.StaticWebExtractor._get_page_HTML")
@patch("metaphor.static_web.extractor.embed_documents")
@patch("metaphor.static_web.extractor.map_metadata")
@pytest.mark.asyncio
async def test_infinite_recursion(
    mock_map_metadata: MagicMock,
    mock_embed_docs: MagicMock,
    mock_get_HTML: MagicMock,
    static_web_extractor,
    test_root_dir: str,
):
    mock_map_metadata.return_value = []
    mock_embed_docs.return_value = []

    # Mock pages appropriately
    page_folder = f"{test_root_dir}/static_web/sample_pages"

    mock_get_HTML.side_effect = [
        load_text(f"{page_folder}/main.html"),
        load_text(f"{page_folder}/page1.html"),
        load_text(f"{page_folder}/page2.html"),
        load_text(f"{page_folder}/page3.html"),
        load_text(f"{page_folder}/page4.html"),
    ]

    # Initialize extractor attributes for infinite recursion test
    # page1 has a backlink to main, so we should not see multiple instances
    static_web_extractor.target_URLs = ["https://example.com/main"]
    static_web_extractor.target_depths = [2]

    await static_web_extractor.extract()

    assert len(static_web_extractor.visited_pages) == 5
    assert len(static_web_extractor.documents) == 5


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
    mock_vector_store_index = MagicMock()

    mock_vector_store_index.storage_context.to_dict.return_value = {
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

    mock_embed_docs.return_value = mock_vector_store_index

    mock_process_subpages.return_value = None

    mock_get_HTML.return_value = "<html><head></head><body><h1>Hello World!</h1><a href='/test'>Link</a></body></html>"

    events = await static_web_extractor.extract()

    assert [e.to_dict() for e in events] == load_json(
        f"{test_root_dir}/static_web/expected.json"
    )
