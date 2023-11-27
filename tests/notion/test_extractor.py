import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings import embed_documents
from metaphor.notion.config import NotionRunConfig
from metaphor.notion.extractor import NotionExtractor

# what things should be tested here?


def dummy_config():
    return NotionRunConfig(
        notion_api_tok="notion_tok",
        openai_api_tok="openai_tok",
        notion_api_version="2022-06-08",
        output=OutputConfig(),
    )


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return


# need to put some thought into how i am mocking test inputs and outputs
# and how to test the extractor methodology
@patch("requests.get")
@pytest.mark.asyncio
async def test_extractor(mock_get: MagicMock, test_root_dir: str):
    assert True
