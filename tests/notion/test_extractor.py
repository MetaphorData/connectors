import json
from unittest.mock import MagicMock, patch

import pytest
from llama_index import Document

from metaphor.common.base_config import OutputConfig
from metaphor.notion.config import NotionRunConfig
from metaphor.notion.extractor import NotionExtractor
from tests.test_utils import load_json

dummy_config = NotionRunConfig(
    notion_api_token="notion_api_token",
    azure_openAI_key="azure_openAI_key",
    azure_openAI_version="azure_openAI_version",
    azure_openAI_endpoint="azure_openAI_endpoint",
    azure_openAI_model="text-embedding-ada-002",
    azure_openAI_model_name="EmbeddingModel",
    include_text=True,
    notion_api_version="2022-06-08",
    output=OutputConfig(),
)


@pytest.fixture
def notion_extractor():
    extractor = NotionExtractor(config=dummy_config)
    return extractor


@pytest.fixture
def mock_notion_reader():
    notion_reader = MagicMock()
    # Mocking load_data to return sample documents
    notion_reader.load_data.return_value = sample_raw_documents
    return notion_reader


sample_raw_documents = [
    Document(
        doc_id="abcd1234",
        embedding=None,
        extra_info={
            "db_id": "database1",
            "platform": "notion",
            "page_id": "efgh5678",
            "link": "https://metaphor.io",
            "lastRefreshed": "2023-12-11 00:00:00.000000",
        },
        hash="1111",
        text="Hello World!",
        start_char_idx=None,
        end_char_idx=None,
        text_template="{metadata_str}\n\n{content}",
        metadata_template="{key}: {value}",
        metadata_seperator="\n",
    )
]

sample_documents = [
    Document(
        doc_id="abcd1234",
        embedding=None,
        extra_info={
            "dbId": "database1",
            "platform": "notion",
            "pageId": "efgh5678",
            "link": "https://metaphor.io",
            "lastRefreshed": "2023-12-11 00:00:00.000000",
        },
        hash="1111",
        text="Hello World!",
        start_char_idx=None,
        end_char_idx=None,
        text_template="{metadata_str}\n\n{content}",
        metadata_template="{key}: {value}",
        metadata_seperator="\n",
    )
]


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return


# test for _get_databases
@patch("requests.post")
@pytest.mark.asyncio
async def test_get_databases(
    mock_post: MagicMock,
    test_root_dir: str,
) -> None:
    mock_post_val = MagicMock()

    mock_post_val.content = json.dumps(
        {
            "results": [
                {"id": 12345},
                {"id": 56789},
            ]
        }
    )

    mock_post.return_value = mock_post_val

    extractor = NotionExtractor(config=dummy_config)

    dbs = extractor._get_databases()

    assert dbs == [12345, 56789]


# test for _get_all_documents
def test_get_all_documents(notion_extractor, mock_notion_reader):
    # Set db_id
    notion_extractor.db_ids = ["db1"]
    notion_extractor.NotionReader = mock_notion_reader

    # Call the method
    documents = notion_extractor._get_all_documents()

    # Assertions
    assert len(documents) == len(
        mock_notion_reader.load_data.return_value
    ), "Should return same number of documents as loaded"
    for doc in documents:
        assert "dbId" in doc.metadata, "Each document should have 'dbId' in metadata"
        assert (
            "platform" in doc.metadata
        ), "Each document should have 'platform' in metadata"
        assert doc.metadata["platform"] == "notion", "The platform should be Notion"
        assert "link" in doc.metadata, "Each document should have 'link' in metadata"
        assert (
            "lastRefreshed" in doc.metadata
        ), "Each document should have 'lastRefreshed' in metadata"


# test for extract
@patch("metaphor.notion.extractor.embed_documents")
@patch("metaphor.notion.NotionExtractor._get_databases")
@patch("metaphor.notion.NotionExtractor._get_all_documents")
@pytest.mark.asyncio
async def test_extractor(
    mock_get_dbs: MagicMock,
    mock_get_docs: MagicMock,
    mock_embed_docs: MagicMock,
    test_root_dir: str,
) -> None:
    # mock db retrieve
    mock_get_dbs.return_value = ["database1"]

    # mock doc retrieve
    mock_get_docs.return_value = sample_documents

    # mock VectorStoreIndex creation
    mock_VSI = MagicMock()

    mock_VSI.storage_context.to_dict.return_value = {
        "vector_store": {
            "default": {
                "embedding_dict": {"abcd1234": [0.1, 0.2, 0.3, 0.4]},
                "metadata_dict": {
                    "abcd1234": {
                        "dbId": "database1",
                        "platform": "notion",
                        "pageId": "efgh5678",
                        "link": "https://metaphor.io",
                        "lastRefreshed": "2023-12-11 00:00:00.000000",
                    }
                },
            }
        },
        "doc_store": {
            "docstore/data": {"abcd1234": {"__data__": {"text": "Hello World!"}}}
        },
    }

    # mock embed docs
    mock_embed_docs.return_value = mock_VSI

    extractor = NotionExtractor(config=dummy_config)

    events = await extractor.extract()

    assert events == load_json(f"{test_root_dir}/notion/expected.json")
