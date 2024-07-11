from metaphor.common.embeddings_config import (
    AzureOpenAIConfig,
    EmbeddingModelConfig,
    OpenAIConfig,
)


def test_default_initialization():
    config = EmbeddingModelConfig()
    assert config.azure_openai.key == ""
    assert config.azure_openai.endpoint == ""
    assert config.azure_openai.version == "2024-03-01-preview"
    assert config.azure_openai.model == "text-embedding-3-small"
    assert config.azure_openai.model_name == "Embedding_3_small"
    assert config.openai.key == ""
    assert config.openai.model == "text-embedding-3-small"
    assert config.chunk_size == 512
    assert config.chunk_overlap == 50


def test_update_with_valid_keys():
    config = EmbeddingModelConfig()
    updates = {
        "azure-openai": {
            "key": "new_key",
            "endpoint": "new_endpoint",
        },
        "openai": {
            "key": "new_openai_key",
        },
    }
    config.update(updates)
    assert config.azure_openai.key == "new_key"
    assert config.azure_openai.endpoint == "new_endpoint"
    assert config.openai.key == "new_openai_key"
    assert config.chunk_size == 512  # unchanged because it has a default value
    assert config.azure_openai.version == "2024-03-01-preview"  # unchanged
    assert config.azure_openai.model == "text-embedding-3-small"  # unchanged


def test_update_with_invalid_keys():
    config = EmbeddingModelConfig()
    updates = {"non_existent_key": "some_value", "another_invalid_key": 12345}
    config.update(updates)
    print(config)
    assert not hasattr(config, "non_existent_key")
    assert not hasattr(config, "another_invalid_key")


def test_partial_update():
    config = EmbeddingModelConfig(azure_openai=AzureOpenAIConfig(key="initial_key"))
    updates = {
        "azure-openai": {
            "key": "updated_key",  # this should not overwrite
            "endpoint": "updated_endpoint",
        },
    }
    config.update(updates)
    assert config.azure_openai.key == "initial_key"  # unchanged
    assert config.azure_openai.endpoint == "updated_endpoint"
    assert config.azure_openai.version == "2024-03-01-preview"  # unchanged
    assert config.azure_openai.model == "text-embedding-3-small"  # unchanged


def test_update_preserves_non_empty_values():
    config = EmbeddingModelConfig(
        azure_openai=AzureOpenAIConfig(
            key="existing_key", endpoint="existing_endpoint"
        ),
        openai=OpenAIConfig(key="existing_openai_key"),
    )
    updates = {
        "azure-openai": {
            "key": "new_key",  # this should not overwrite
            "endpoint": "new_endpoint",  # this should not overwrite
        },
        "openai": {
            "key": "new_openai_key",  # this should not overwrite
        },
    }
    config.update(updates)
    assert config.azure_openai.key == "existing_key"  # unchanged
    assert config.azure_openai.endpoint == "existing_endpoint"  # unchanged
    assert config.openai.key == "existing_openai_key"  # unchanged
    assert config.azure_openai.version == "2024-03-01-preview"  # unchanged
    assert config.azure_openai.model == "text-embedding-3-small"  # unchanged
