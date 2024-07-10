from unittest.mock import MagicMock, patch

import pytest
from llama_index.core import Document

from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.confluence.config import ConfluenceRunConfig
from metaphor.confluence.extractor import ConfluenceExtractor
from tests.test_utils import load_json

dummy_config = ConfluenceRunConfig(
    confluence_base_URL="https://test.atlassian.net/wiki",
    confluence_cloud=True,
    select_method="label",
    confluence_username="test@metaphor.io",
    confluence_token="token",
    space_key="KB",
    embed_model_config=EmbeddingModelConfig(),
    include_text=True,
    output=OutputConfig(),
)


@pytest.fixture
def confluence_extractor():
    extractor = ConfluenceExtractor(config=dummy_config)
    return extractor


@pytest.fixture
def mock_confluence_reader():
    confluence_reader = MagicMock()
    confluence_reader.load_data.return_value = sample_raw_documents
    return confluence_reader


sample_raw_documents = [
    Document(  # type: ignore[call-arg]
        doc_id="5678",
        embedding=None,
        extra_info={
            "title": "Hello World!",
            "page_id": "5678",
            "status": "current",
            "url": "https://metaphor.io",
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

sample_formatted_documents = [
    Document(  # type: ignore[call-arg]
        id_="5678",
        embedding=None,
        metadata={
            "title": "Hello World!",
            "status": "current",
            "pageId": "5678",
            "platform": "confluence",
            "link": "https://metaphor.io",
            "lastRefreshed": "2024-04-03 00:00:00.000000",
        },
        excluded_embed_metadata_keys=[],
        excluded_llm_metadata_keys=[],
        relationships={},
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


# test for _load_confluence_data
@patch("metaphor.confluence.extractor.datetime")
def test_load_confluence_data(
    mock_dt: MagicMock, mock_confluence_reader, confluence_extractor
):
    mock_dt.now.return_value = "2024-04-03 00:00:00.000000"
    confluence_extractor.confluence_reader = mock_confluence_reader

    documents = confluence_extractor._load_confluence_data()

    assert documents == sample_formatted_documents


# test for extract
@patch("metaphor.confluence.ConfluenceExtractor._load_confluence_data")
@patch("metaphor.confluence.extractor.embed_documents")
@pytest.mark.asyncio
async def test_extract(
    mock_embed_docs: MagicMock,
    mock_load_data: MagicMock,
    confluence_extractor,
    mock_confluence_reader,
    test_root_dir: str,
):
    mock_VSI = MagicMock()

    mock_VSI.storage_context.to_dict.return_value = {
        "vector_store": {
            "default": {
                "embedding_dict": {"5678": [0.1, 0.2, 0.3, 0.4]},
                "metadata_dict": {
                    "5678": {
                        "title": "Hello World!",
                        "status": "current",
                        "platform": "confluence",
                        "pageId": "5678",
                        "link": "https://metaphor.io",
                        "lastRefreshed": "2024-04-03 00:00:00.000000",
                    }
                },
            }
        },
        "doc_store": {
            "docstore/data": {"5678": {"__data__": {"text": "Hello World!"}}}
        },
    }

    # mock embed docs
    mock_embed_docs.return_value = mock_VSI

    # mock return value of load_data
    mock_load_data.return_value = sample_formatted_documents

    # mock confluence reader
    confluence_extractor.confluence_reader = mock_confluence_reader

    events = await confluence_extractor.extract()

    assert [e.to_dict() for e in events] == load_json(
        f"{test_root_dir}/confluence/expected.json"
    )
