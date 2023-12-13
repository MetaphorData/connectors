from metaphor.common.embeddings import clean_text, map_metadata


def test_clean_text(test_root_dir: str) -> None:
    assert clean_text("  Hello  World  ") == "Hello World"
    assert clean_text("\nNew\nLine\n") == "New Line"
    assert clean_text("\tTab\tCharacter\t") == "Tab Character"
    assert clean_text("\tTab\tCharacter\t") == "Tab Character"
    assert clean_text(" Multiple    Spaces ") == "Multiple Spaces"


def test_map_metadata(test_root_dir: str) -> None:
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
    result = map_metadata(
        embedding_dict_example, metadata_dict_example, True, doc_store_example
    )

    # Check type
    assert isinstance(result, list) or isinstance(
        result, tuple
    ), "Output should be a collection (list or tuple)"

    # Check length
    assert len(result) == len(
        embedding_dict_example
    ), "Output length should match the number of nodes"

    # Check content structure
    for item in result:
        assert (
            "externalSearchDocument" in item
        ), "Each item should have a key 'externalSearchDocument'"
        doc = item["externalSearchDocument"]

        # Check for required fields
        assert "nodeId" in doc, "Document should contain nodeId"
        assert "embedding" in doc, "Document should contain embedding"
        assert "pageId" in doc, "Document should contain pageId"
        assert "lastRefreshed" in doc, "Document should contain lastRefreshed"
        assert "metadata" in doc, "Document should contain metadata"

        # Check if values are correctly mapped
        assert (
            doc["nodeId"] in embedding_dict_example
        ), "nodeId should be in the embedding dictionary"
        assert (
            doc["embedding"] == embedding_dict_example[doc["nodeId"]]["embedding"]  # type: ignore[call-overload]
        ), "Embedding should match input data"
        assert (
            doc["pageId"] == metadata_dict_example[doc["nodeId"]]["pageId"]
        ), "PageId should match input data"
        assert (
            doc["lastRefreshed"]
            == metadata_dict_example[doc["nodeId"]]["lastRefreshed"]
        ), "lastRefreshed should match input data"
        assert (
            doc["metadata"] == metadata_dict_example[doc["nodeId"]]
        ), "Metadata should match input data"

        # Check for optional 'embeddingString'
        if "embeddingString" in doc:
            assert (
                doc["embeddingString"]
                == doc_store_example[doc["nodeId"]]["__data__"]["text"]
            ), "EmbeddingString should match input data"
