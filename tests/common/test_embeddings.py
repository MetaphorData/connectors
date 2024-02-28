from copy import deepcopy as dc
from unittest.mock import MagicMock

import pytest

from metaphor.common.embeddings import map_metadata, sanitize_text
from metaphor.models.metadata_change_event import ExternalSearchDocument


@pytest.fixture
def example_vsi_data() -> tuple:
    # Example mock data
    embedding_dict_example = {"node1": [0.1, 0.2, 0.3], "node2": [0.4, 0.5, 0.6]}
    metadata_dict_example = {
        "node1": {
            "title": "Hello World!",
            "pageId": "page1",
            "lastRefreshed": "2023-01-01",
        },
        "node2": {
            "title": "Hello World!",
            "pageId": "page2",
            "lastRefreshed": "2023-01-02",
        },
    }
    doc_store_example = {
        "node1": {"__data__": {"text": "Text for node1"}},
        "node2": {"__data__": {"text": "Text for node2"}},
    }

    # Mock VSI object
    mock_vsi = MagicMock()
    mock_vsi.storage_context.to_dict.return_value = {
        "vector_store": {
            "default": {
                "embedding_dict": embedding_dict_example,
                "metadata_dict": metadata_dict_example,
            }
        },
        "doc_store": {"docstore/data": doc_store_example},
    }

    return embedding_dict_example, metadata_dict_example, doc_store_example, mock_vsi


def test_sanitize_text(test_root_dir: str) -> None:
    assert sanitize_text("  Hello  World  ") == "Hello World"
    assert sanitize_text("\nNew\nLine\n") == "New Line"
    assert sanitize_text("\tTab\tCharacter\t") == "Tab Character"
    assert sanitize_text("\rCarriage\rReturn\r") == "Carriage Return"
    assert sanitize_text(" Multiple    Spaces ") == "Multiple Spaces"


def test_map_metadata_string(example_vsi_data):
    embedding_dict, metadata_dict, doc_store, mock_vsi = dc(example_vsi_data)
    # Call the function with the mock VSI and include_text as True
    results_with_string = map_metadata(mock_vsi, True)

    embedding_dict, metadata_dict, doc_store, mock_vsi = dc(example_vsi_data)
    # Call the function with the mock VSI and include_text as False
    results_without_string = map_metadata(mock_vsi, False)

    # Check type
    assert isinstance(results_with_string, list), "Output should be a list"
    assert all(
        [isinstance(el, ExternalSearchDocument) for el in results_with_string]
    ), "Each should be an ExternalSearchDocument"

    # Check length
    assert len(results_with_string) == len(
        embedding_dict
    ), "Output length should match the number of nodes"

    # Check content structure
    for external_document in results_with_string:
        assert (
            external_document.entity_id
        ), "Each item should have 'entityId' in its 'externalSearchDocument'"
        assert (
            external_document.embedding_1
        ), "Each item should have 'embedding_1' in its 'externalSearchDocument'"
        assert (
            external_document.page_id
        ), "Each item should have 'pageId' in its 'externalSearchDocument'"
        assert (
            external_document.last_refreshed
        ), "Each item should have 'lastRefreshed' in its 'externalSearchDocument'"
        assert (
            external_document.metadata
        ), "Each item should have 'metadata' in its 'externalSearchDocument'"

        # Additional checks for 'include_text'
        if external_document.embedded_string_1:
            nodeid = external_document.entity_id.lower().split("~")[-1]
            text = doc_store[nodeid]["__data__"]["text"]
            title = metadata_dict[nodeid]["title"]

            assert (
                external_document.embedded_string_1 == f"Title: {title}\n{text}"
            ), "embeddedString_1 should match the text in doc_store"

    assert all(
        [isinstance(el, ExternalSearchDocument) for el in results_without_string]
    ), "Each should be an ExternalSearchDocument"

    # Check length
    assert len(results_without_string) == len(
        embedding_dict
    ), "Output length should match the number of nodes"

    # Check that 'embeddedString_1' is not included when include_text is False
    for external_document in results_without_string:
        assert (
            not external_document.embedded_string_1
        ), "embeddedString_1 should not be present when include_text is False"
