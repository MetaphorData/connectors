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


def mock_graph_client() -> AsyncMock:
    mock_GraphServiceClient = AsyncMock()

    # Mock for _get_site_collections()
    # Mock the get() for sites to return site collections
    mock_GraphServiceClient.sites.get.return_value = MagicMock(
        return_value=MagicMock(
            value=[MagicMock(web_url="https://sharepoint.sample.com/")]
        )
    )

    # Mock for _get_sites
    # Mock the get_all_sites.get() to return site details
    site_details = [
        MagicMock(id="siteId_1"),
        MagicMock(id="siteId_2"),
    ]
    # Manually setting the name attribute post-creation
    site_details[0].configure_mock(name="TestSharePointSite")
    site_details[1].configure_mock(name="TestSite2")

    mock_GraphServiceClient.sites.get_all_sites.get.return_value = MagicMock(
        value=site_details
    )

    # Mock for _get_site_pages
    page_details = MagicMock(
        value=[
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
    )

    mock_by_site_id = MagicMock()
    mock_by_site_id.pages.graph_site_page.get = AsyncMock(return_value=page_details)
    mock_GraphServiceClient.sites.by_site_id = MagicMock(return_value=mock_by_site_id)

    # Mock for _get_webParts_HTML
    web_parts_details = [
        MagicMock(inner_html="<div>Hello</div>"),
        MagicMock(inner_html="<div>World</div>"),
    ]

    return mock_GraphServiceClient


@pytest.fixture
@patch("metaphor.sharepoint.extractor.ClientSecretCredential")
@patch("metaphor.sharepoint.extractor.GraphServiceClient")
def sharepoint_extractor(mock_ClientSecretCredential, mock_GraphServiceClient):
    mock_ClientSecretCredential = MagicMock()
    mock_GraphServiceClient = mock_graph_client()
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
async def test_get_sites(sharepoint_extractor: SharepointExtractor):
    sites = await sharepoint_extractor._get_sites()
    expected_sites = {"TestSharePointSite": "siteId_1", "TestSite2": "siteId_2"}
    assert sites == expected_sites


# test for _get_site_pages
@pytest.mark.asyncio
async def test_get_site_pages(sharepoint_extractor: SharepointExtractor):
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
async def test_get_webParts_HTML(sharepoint_extractor: SharepointExtractor):
    html_content = await sharepoint_extractor._get_webParts_HTML("siteId_1", "page1")
    expected_html = "<div>Hello</div>\n<div>World</div>"
    assert html_content == expected_html


# test for extract
