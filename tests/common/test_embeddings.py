from unittest.mock import MagicMock

from metaphor.common.embeddings import clean_text, map_metadata


def test_clean_text(test_root_dir: str) -> None:
    assert clean_text("  Hello  World  ") == "Hello World"
    assert clean_text("\nNew\nLine\n") == "New Line"
    assert clean_text("\tTab\tCharacter\t") == "Tab Character"
    assert clean_text("\tTab\tCharacter\t") == "Tab Character"
    assert clean_text(" Multiple    Spaces ") == "Multiple Spaces"


def test_map_metadata_string():
    # Example mock data
    embedding_dict_example = {"node1": [0.1, 0.2, 0.3], "node2": [0.4, 0.5, 0.6]}
    metadata_dict_example = {
        "node1": {"pageId": "page1", "lastRefreshed": "2023-01-01"},
        "node2": {"pageId": "page2", "lastRefreshed": "2023-01-02"},
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

    # Call the function with the mock VSI and include_text as True
    results_with_string = map_metadata(mock_vsi, True)

    # Check type
    assert isinstance(results_with_string, list), "Output should be a list"

    # Check length
    assert len(results_with_string) == len(
        embedding_dict_example
    ), "Output length should match the number of nodes"

    # Check content structure
    for item in results_with_string:
        assert (
            "externalSearchDocument" in item
        ), "Each item should have 'externalSearchDocument' key"
        external_document = item["externalSearchDocument"]
        assert (
            "entityId" in external_document
        ), "Each item should have 'entityId' in its 'externalSearchDocument'"
        assert (
            "embedding_1" in external_document
        ), "Each item should have 'embedding_1' in its 'externalSearchDocument'"
        assert (
            "pageId" in external_document
        ), "Each item should have 'pageId' in its 'externalSearchDocument'"
        assert (
            "lastRefreshed" in external_document
        ), "Each item should have 'lastRefreshed' in its 'externalSearchDocument'"
        assert (
            "metadata" in external_document
        ), "Each item should have 'metadata' in its 'externalSearchDocument'"

        # Additional checks for the 'include_text' condition
        if "embeddedString_1" in external_document:
            text = doc_store_example[external_document["entityId"].split("~")[-1]][
                "__data__"
            ]["text"]
            assert (
                external_document["embeddedString_1"] == text
            ), "embeddedString_1 should match the text in doc_store"

    # Call the function with the mock VSI and include_text as False
    results_without_string = map_metadata(mock_vsi, False)

    # Check length
    assert len(results_without_string) == len(
        embedding_dict_example
    ), "Output length should match the number of nodes"

    # Check that 'embeddedString_1' is not included when include_text is False
    for item in results_without_string:
        assert (
            "externalSearchDocument" in item
        ), "Each item should have 'externalSearchDocument' key"
        external_document = item["externalSearchDocument"]
        assert (
            "embeddedString_1" not in external_document
        ), "embeddedString_1 should not be present when include_text is False"
