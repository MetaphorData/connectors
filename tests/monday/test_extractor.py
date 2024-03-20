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
        doc_id="5678",
        embedding=None,
        extra_info={
            "title": "Hello World!",
            "board": 1234,
            "boardName": "Hello World!",
            "link": "https://metaphor.io",
            "page_id": "5678",
            "platform": "monday",
            "lastRefreshed": "2023-12-11 00:00:00.000000",
        },
        hash="1111",
        text="Board Name: Hello World!\nTitle: Hello World!",
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
def test_get_board_columns_success(
    mock_post: MagicMock, monday_extractor, mock_responses
):
    mock_post.return_value.json.return_value = mock_responses["columns"]
    monday_extractor.current_board = 1234
    monday_extractor.current_board_name = "Hello World!"

    columns = monday_extractor._get_board_columns(1234)
    assert columns == {
        "name": "Name",
        "columns_properties3": "Columns/Properties",
        "descriptions": "Descriptions",
    }, "Column names and types should be correct, and ignores person column"


# test for _get_board_columns exception
@patch("requests.post")
def test_get_board_columns_failure(mock_post: MagicMock, monday_extractor):
    mock_post.side_effect = requests.HTTPError

    with pytest.raises(Exception):  # HTTPError is handled correctly
        monday_extractor._get_board_columns(1234)


# test for _get_board_items success
@patch("requests.post")
def test_get_board_items_success(
    mock_post: MagicMock, monday_extractor, mock_responses
):
    mock_post.return_value.json.return_value = mock_responses["items"]
    monday_extractor.current_board = 1234
    monday_extractor.current_board_name = "Hello World!"

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
def test_get_board_items_failure(mock_post: MagicMock, monday_extractor):
    mock_post.side_effect = requests.HTTPError

    with pytest.raises(Exception):  # HTTPError is handled correctly
        monday_extractor._get_board_items(1234)


# test for _get_monday_doc success
@patch("requests.post")
def test_get_monday_doc_success(mock_post: MagicMock, monday_extractor, mock_responses):
    mock_post.return_value.json.return_value = mock_responses["document"]

    doc_text = monday_extractor._get_monday_doc(1234)

    assert doc_text == "Hello\nWorld!\n"


# test for _get_monday_doc exception
@patch("requests.post")
def test_get_monday_doc_failure(mock_post: MagicMock, monday_extractor):
    mock_post.side_effect = requests.HTTPError

    with pytest.raises(Exception):  # HTTPError is handled correctly
        monday_extractor._get_monday_doc(1234)


# test for _construct_items_documents
@patch("requests.post")
def test_construct_items_documents(
    mock_post: MagicMock, monday_extractor, mock_responses
):
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


@patch("metaphor.monday.extractor.MondayExtractor._get_board_columns")
@patch("metaphor.monday.extractor.MondayExtractor._get_board_items")
def test_construct_items_documents_with_updates(
    mock_get_board_items, mock_get_board_columns, monday_extractor, mock_responses
):
    # Mock responses for columns and items to include updates
    mock_get_board_columns.return_value = mock_responses["columns"]
    mock_get_board_items.return_value = [
        {
            "id": "123",
            "name": "Item Name",
            "updates": [
                {"text_body": "First \t update"},
                {"text_body": "Second \t update"},
            ],
            "column_values": [],
            "url": "https://example.com/item/123",
        }
    ]

    columns = mock_responses["columns"]

    monday_extractor.current_board = 1234
    monday_extractor.current_board_name = "Hello World!"

    documents = monday_extractor._construct_items_documents(
        mock_get_board_items.return_value, columns
    )

    assert len(documents) == 1, "One document should be constructed"
    document_text = documents[0].text
    expected_text = (
        "Board Name: Hello World!\nUpdate: First update\nUpdate: Second update\n"
    )
    assert (
        document_text == expected_text
    ), f"Document text should include updates: {expected_text}"


# test_extract for MondayExtractor
@patch("metaphor.monday.extractor.MondayExtractor._construct_items_documents")
@patch("metaphor.monday.extractor.MondayExtractor._get_monday_doc")
@patch("metaphor.monday.extractor.MondayExtractor._get_board_items")
@patch("metaphor.monday.extractor.MondayExtractor._get_board_columns")
@patch("metaphor.monday.extractor.embed_documents")
@pytest.mark.asyncio
async def test_extract(
    mock_embed_documents: MagicMock,
    mock_get_board_columns: MagicMock,
    mock_get_board_items: MagicMock,
    mock_get_monday_doc: MagicMock,
    mock_construct_items_documents: MagicMock,
    monday_extractor,
    mock_responses,
    test_root_dir,
):
    # mock VectorStoreIndex creation
    mock_VSI = MagicMock()

    mock_VSI.storage_context.to_dict.return_value = {
        "vector_store": {
            "default": {
                "embedding_dict": {"5678": [0.1, 0.2, 0.3, 0.4]},
                "metadata_dict": {
                    "5678": {
                        "title": "Hello World!",
                        "board": 1234,
                        "boardName": "Hello World!",
                        "link": "https://metaphor.io",
                        "pageId": "5678",
                        "platform": "monday",
                        "lastRefreshed": "2023-12-11 00:00:00.000000",
                    }
                },
            }
        },
        "doc_store": {
            "docstore/data": {
                "5678": {"__data__": {"text": "Board Name: Hello World!"}}
            }
        },
    }

    # Mocking method responses
    mock_embed_documents.return_value = mock_VSI
    mock_get_board_columns.return_value = mock_responses["columns"]
    mock_get_board_items.return_value = mock_responses["items"]
    mock_get_monday_doc.return_value = "Document content"
    mock_construct_items_documents.return_value = sample_raw_documents

    events = await monday_extractor.extract()

    assert [e.to_dict() for e in events] == load_json(
        f"{test_root_dir}/monday/expected.json"
    )


@patch("metaphor.monday.extractor.MondayExtractor._construct_items_documents")
@patch("metaphor.monday.extractor.MondayExtractor._get_monday_doc")
@patch("metaphor.monday.extractor.MondayExtractor._get_board_items")
@patch("metaphor.monday.extractor.MondayExtractor._get_board_columns")
@patch("metaphor.monday.extractor.embed_documents")
@pytest.mark.asyncio
async def test_extract_multiple_boards(
    mock_embed_documents,
    mock_get_board_columns,
    mock_get_board_items,
    mock_get_monday_doc,
    mock_construct_items_documents,
    monday_extractor,
    mock_responses,
    test_root_dir,
):
    # Adjust the monday_extractor to have multiple boards for testing
    monday_extractor.boards = [1234, 5678]

    # Assuming each board has a different set of columns and items to be mocked
    mock_get_board_columns.side_effect = [
        mock_responses["columns"],
        mock_responses["columns"],
    ]
    mock_get_board_items.side_effect = [
        mock_responses["items"],
        mock_responses["items"],
    ]
    mock_construct_items_documents.side_effect = [
        sample_raw_documents,
        sample_raw_documents,
    ]

    await monday_extractor.extract()

    # Verify methods called with correct arguments for each board
    assert mock_get_board_columns.call_args_list == [((1234,),), ((5678,),)]
    assert mock_get_board_items.call_args_list == [
        ((1234, mock_responses["columns"]),),
        ((5678, mock_responses["columns"]),),
    ]
    assert mock_construct_items_documents.call_count == 2
