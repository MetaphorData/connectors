from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.notion.config import NotionRunConfig


def dummy_config():
    return NotionRunConfig(
        notion_api_tok="notion_api_tok",
        azure_openAI_key="azure_openAI_key",
        azure_openAI_ver="azure_openAI_ver",
        azure_openAI_endpoint="azure_openAI_endpoint",
        azure_openAI_model="text-embedding-ada-002",
        azure_openAI_model_name="EmbeddingModel",
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
