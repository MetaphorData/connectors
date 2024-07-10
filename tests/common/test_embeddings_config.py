from metaphor.common.embeddings_config import EmbeddingModelConfig


def test_default_initialization():
    config = EmbeddingModelConfig()
    assert config.azure_openAI_key == ""
    assert config.azure_openAI_endpoint == ""
    assert config.azure_openAI_version == "2024-03-01-preview"
    assert config.azure_openAI_model == "text-embedding-3-small"
    assert config.azure_openAI_model_name == "Embedding_3_small"
    assert config.openAI_key == ""
    assert config.openAI_model == "text-embedding-3-small"
    assert config.chunk_size == 512
    assert config.chunk_overlap == 50


def test_update_with_valid_keys():
    config = EmbeddingModelConfig()
    updates = {
        "azure_openAI_key": "new_key",
        "azure_openAI_endpoint": "new_endpoint",
        "openAI_key": "new_openai_key",
    }
    config.update(updates)
    assert config.azure_openAI_key == "new_key"
    assert config.azure_openAI_endpoint == "new_endpoint"
    assert config.openAI_key == "new_openai_key"
    assert config.chunk_size == 512  # unchanged because it has a default value
    assert config.azure_openAI_version == "2024-03-01-preview"  # unchanged
    assert config.azure_openAI_model == "text-embedding-3-small"  # unchanged


def test_update_with_invalid_keys():
    config = EmbeddingModelConfig()
    updates = {"non_existent_key": "some_value", "another_invalid_key": 12345}
    config.update(updates)
    print(config)
    assert not hasattr(config, "non_existent_key")
    assert not hasattr(config, "another_invalid_key")


def test_partial_update():
    config = EmbeddingModelConfig(azure_openAI_key="initial_key")
    updates = {
        "azure_openAI_key": "updated_key",  # this should not overwrite
        "azure_openAI_endpoint": "updated_endpoint",
    }
    config.update(updates)
    assert config.azure_openAI_key == "initial_key"  # unchanged
    assert config.azure_openAI_endpoint == "updated_endpoint"
    assert config.azure_openAI_version == "2024-03-01-preview"  # unchanged
    assert config.azure_openAI_model == "text-embedding-3-small"  # unchanged


def test_update_preserves_non_empty_values():
    config = EmbeddingModelConfig(
        azure_openAI_key="existing_key",
        azure_openAI_endpoint="existing_endpoint",
        openAI_key="existing_openai_key",
    )
    updates = {
        "azure_openAI_key": "new_key",  # this should not overwrite
        "azure_openAI_endpoint": "new_endpoint",  # this should not overwrite
        "openAI_key": "new_openai_key",  # this should not overwrite
    }
    config.update(updates)
    assert config.azure_openAI_key == "existing_key"  # unchanged
    assert config.azure_openAI_endpoint == "existing_endpoint"  # unchanged
    assert config.openAI_key == "existing_openai_key"  # unchanged
    assert config.azure_openAI_version == "2024-03-01-preview"  # unchanged
    assert config.azure_openAI_model == "text-embedding-3-small"  # unchanged
