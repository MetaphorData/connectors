import json
from unittest.mock import MagicMock, patch

import pytest
import requests
from llama_index.core import Document

from metaphor.common.base_config import OutputConfig
from metaphor.monday.config import MondayRunConfig
from metaphor.monday.extractor import MondayExtractor
from tests.test_utils import load_json

dummy_config = MondayRunConfig(
    monday_api_key="monday_api_key",
    monday_api_version="monday_api_version",
    boards=[],
    azure_openAI_key="azure_openAI_key",
    azure_openAI_version="azure_openAI_version",
    azure_openAI_endpoint="azure_openAI_endpoint",
    azure_openAI_model="text-embedding-ada-002",
    azure_openAI_model_name="Embedding_ada002",
    include_text=True,
    output=OutputConfig(),
)


@pytest.fixture
def mock_responses(test_root_dir):
    cols = load_json(f"{test_root_dir}/monday/mock_get_columns.json")
    items = load_json(f"{test_root_dir}/monday/mock_get_items.json")
    doc = load_json(f"{test_root_dir}/monday/mock_get_doc.json")

    return {"columns": cols, "items": items, "document": doc}


@pytest.fixture
def monday_extractor():
    extractor = MondayExtractor(config=dummy_config)
    return extractor


sample_raw_documents = [
    Document(  # type: ignore[call-arg]
        doc_id="abcd1234",
        embedding=None,
        extra_info={
            "title": "Hello World!",
            "board": 1234,
            "link": "https://metaphor.io",
            "page_id": 5678,
            "platform": "monday",
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


# test for _get_board_columns success
@patch("requests.post")
def test_get_board_columns_success(mock_post, monday_extractor, mock_responses):
    mock_post.return_value.json.return_value = mock_responses["columns"]

    columns = monday_extractor._get_board_columns(1234)
    assert columns == {
        "name": "Name",
        "columns_properties3": "Columns/Properties",
        "descriptions": "Descriptions",
    }, "Column names and types should be correct, and ignores person column"


# test for _get_board_columns exception
@patch("requests.post")
def test_get_board_columns_failure(mock_post, monday_extractor):
    mock_post.side_effect = requests.HTTPError()

    with pytest.raises(Exception):  # HTTPError is handled correctly
        monday_extractor._get_board_columns(1234)


# test for _get_board_items success
@patch("requests.post")
def test_get_board_items_success(mock_post, monday_extractor, mock_responses):
    mock_post.return_value.json.return_value = mock_responses["items"]

    items = monday_extractor._get_board_items(1234, columns=mock_responses["columns"])

    assert items == [
        {
            "id": "1234",
            "name": "Item Name",
            "updates": [],
            "column_values": [
                {
                    "id": "columns_properties3",
                    "text": "Hello World!",
                    "value": '{"ids":[2],"changed_at":"2024-03-11T21:11:31.582Z"}',
                },
                {"id": "descriptions", "text": "text", "value": "value"},
            ],
            "url": "https://test-test.monday.com/boards/1234",
        },
        {
            "id": "5678",
            "name": "Item Name",
            "updates": [],
            "column_values": [
                {
                    "id": "columns_properties3",
                    "text": "text",
                    "value": '{"ids":[5],"changed_at":"2024-03-11T21:11:37.116Z"}',
                },
                {"id": "descriptions", "text": "Hello World!", "value": None},
            ],
            "url": "https://test-test.monday.com/boards/1234",
        },
    ]


# test for _get_board_items exception
@patch("requests.post")
def test_get_board_items_failure(mock_post, monday_extractor):
    mock_post.side_effect = requests.HTTPError()

    with pytest.raises(Exception):  # HTTPError is handled correctly
        monday_extractor._get_board_items(1234)


# test for _get_monday_doc success
@patch("requests.post")
def test_get_monday_doc_success(mock_post, monday_extractor, mock_responses):
    mock_post.return_value.json.return_value = mock_responses["document"]

    doc_text = monday_extractor._get_monday_doc(1234)

    assert doc_text == "Hello\nWorld!\n"


# test for _get_monday_doc exception
@patch("requests.post")
def test_get_monday_doc_failure(mock_post, monday_extractor):
    mock_post.side_effect = requests.HTTPError()

    with pytest.raises(Exception):  # HTTPError is handled correctly
        monday_extractor._get_monday_doc(1234)


# test for _construct_items_documents
@patch("requests.post")
def test_construct_items_documents(mock_post, monday_extractor, mock_responses):
    mock_post.return_value.json.return_value = mock_responses["columns"]
    columns = monday_extractor._get_board_columns(1234)

    mock_post.return_value.json.return_value = mock_responses["items"]
    items = monday_extractor._get_board_items(1234, columns=mock_responses["columns"])
    monday_extractor.current_board = 1234
    documents = monday_extractor._construct_items_documents(items, columns)

    assert len(documents) == 2, "Appropriate number of Documents constructed"
    assert all(
        [d.extra_info["board"] == 1234 for d in documents]
    ), "All Documents have the correct board number"
    assert all(
        [d.extra_info["platform"] == "monday" for d in documents]
    ), "All Documents have the correct platform"
    assert all([d.extra_info["title"] for d in documents]), "All Documents have a title"
    assert all([d.text for d in documents]), "All Documents have text"
