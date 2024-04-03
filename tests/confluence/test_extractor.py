from unittest.mock import MagicMock, patch

import pytest
import requests
from llama_index.core import Document

from metaphor.common.base_config import OutputConfig
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
    azure_openAI_key="azure_openAI_key",
    azure_openAI_version="azure_openAI_version",
    azure_openAI_endpoint="azure_openAI_endpoint",
    azure_openAI_model="text-embedding-3-small",
    azure_openAI_model_name="Embedding_3_small",
    include_text=True,
    output=OutputConfig(),
)

@pytest.fixture
def confluence_extractor()
    extractor = ConfluenceExtractor(config = dummy_config)
    return extractor

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

class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return
    
# test for _load_confluence_data
def test_load_confluence_data():
    pass