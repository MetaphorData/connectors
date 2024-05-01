from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.sharepoint.config import SharepointRunConfig
from metaphor.sharepoint.extractor import SharepointExtractor
from llama_index.core import Document
from tests.test_utils import load_json

dummy_config = SharepointRunConfig(
    sharepoint_client_id="client_id",
    sharepoint_client_secret="client_secret",
    sharepoint_tenant_id="tenant_id",
    azure_openAI_key="azure_openAI_key",
    azure_openAI_version="azure_openAI_version",
    azure_openAI_endpoint="azure_openAI_endpoint",
    azure_openAI_model="text-embedding-3-small",
    azure_openAI_model_name="Embedding_3_small",
    include_text=True,
    output=OutputConfig(),
)


def mock_graph_client(mock_GraphServiceClient: AsyncMock) -> AsyncMock:
    # endpoints that need to be mocked
    # gsc.sites.get()
    # gsc.sites.get_all_sites.get()
    # gsc.sites.by_site_id.pages.graph_site_page.get()
    # gsc.sites.by_site_id.pages.by_base_site_page_id.graph_site_page.web_parts.get()

    # mock for _get_site_collections
    return_value = [MagicMock(web_url="https://sharepoint.sample.com/")]
    mock_GraphServiceClient.sites.get.return_value = MagicMock(value=return_value)

    # mock for _get_sites
    return_value = [
        MagicMock(id="siteId_1"),
        MagicMock(id="siteId_2"),
    ]

    # manually set name attribute
    return_value[0].configure_mock(name="TestSharePointSite")
    return_value[1].configure_mock(name="TestSite2")

    mock_GraphServiceClient.sites.get_all_sites.get.return_value = MagicMock(
        value=return_value
    )

    # mock for _get_site_pages
    return_value = [
        MagicMock(
            title="Welcome Page",
            id="page1",
            web_url="https://sharepoint.sample.com/pages/page1",
        ),
        MagicMock(
            title="Frequently Asked Questions",
            id="page2",
            web_url="https://sharepoint.sample.com/pages/page2",
        ),
    ]
    mock_GraphServiceClient.by_site_id.return_value.pages.graph_site_page.get.return_value = MagicMock(
        value=return_value
    )

    # mock for _get_webParts_HTML
    return_value = [
        MagicMock(inner_html="<div>Hello</div>"),
        MagicMock(value="<div>Hello</div>"),  # test for not inner_html
        MagicMock(inner_html="<div>World</div>"),
    ]
    mock_GraphServiceClient.by_site_id.return_value.pages.by_base_site_page_id.graph_site_page.web_parts.get.return_value = MagicMock(
        value=return_value
    )

    return mock_GraphServiceClient


@pytest.fixture
@patch("metaphor.sharepoint.extractor.ClientSecretCredential")
@patch("metaphor.sharepoint.extractor.GraphServiceClient")
def sharepoint_extractor(mock_ClientSecretCredential, mock_GraphServiceClient):
    mock_ClientSecretCredential = MagicMock()
    mock_GraphServiceClient = mock_graph_client(AsyncMock())
    extractor = SharepointExtractor(config=dummy_config)
    extractor.ClientSecretCredential = mock_ClientSecretCredential
    extractor.GraphServiceClient = mock_GraphServiceClient
    return extractor


sample_documents = [
    Document(
        text="Sample document text",
        extra_info={
            "title": "Sample Document",
            "platform": "sharepoint/SampleSite",
            "link": "https://sharepoint.sample.com/pages/sample-document",
            "lastRefreshed": "2024-04-03 00:00:00.000000",
            "pageId": "page123",
        },
    )
]


# test for _get_sites
@pytest.mark.asyncio
async def test_get_sites(sharepoint_extractor):
    sites = await sharepoint_extractor._get_sites()
    expected_sites = {"TestSharePointSite": "siteId_1", "TestSite2": "siteId_2"}
    assert sites == expected_sites


# test for _get_site_pages
@pytest.mark.asyncio
async def test_get_site_pages(sharepoint_extractor):
    pages = await sharepoint_extractor._get_site_pages("siteId_1")
    expected_pages = [
        ("Welcome Page", "page1", "https://sharepoint.sample.com/pages/page1"),
        (
            "Frequently Asked Questions",
            "page2",
            "https://sharepoint.sample.com/pages/page2",
        ),
    ]
    assert pages == expected_pages


# test for _get_webParts_HTML
@pytest.mark.asyncio
async def test_get_webParts_HTML(sharepoint_extractor):
    html_content = await sharepoint_extractor._get_webParts_HTML("siteId_1", "page1")
    expected_html = "<div>Hello</div>\n<div>World</div>"
    assert html_content == expected_html


# test for extract