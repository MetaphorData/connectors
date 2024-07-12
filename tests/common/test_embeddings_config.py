from metaphor.common.embeddings_config import (
    AzureOpenAIConfig,
    EmbeddingModelConfig,
    OpenAIConfig,
)


def test_default_initialization():
    config = EmbeddingModelConfig()
    assert config.azure_openai is None or config.azure_openai.key is None
    assert config.azure_openai is None or config.azure_openai.endpoint is None
    assert (
        config.azure_openai is None
        or config.azure_openai.version == "2024-03-01-preview"
    )
    assert (
        config.azure_openai is None
        or config.azure_openai.model == "text-embedding-3-small"
    )
    assert (
        config.azure_openai is None
        or config.azure_openai.deployment_name == "Embedding_3_small"
    )
    assert config.openai is None or config.openai.key is None
    assert config.openai is None or config.openai.model == "text-embedding-3-small"
    assert config.chunk_size == 512
    assert config.chunk_overlap == 50


def test_update_with_valid_keys():
    user_configuration = {"azure_openai": {"key": "key", "endpoint": "endpoint"}}

    config = EmbeddingModelConfig(**user_configuration)

    assert hasattr(config, "azure_openai")
    assert isinstance(config.active_config, AzureOpenAIConfig)
    assert config.active_config.version == AzureOpenAIConfig().version
    assert hasattr(config, "openai")
    assert not getattr(config, "openai")
    assert config.chunk_size == 512
    assert config.chunk_overlap == 50


def test_update_with_invalid_keys():
    updates = {"non_existent_key": "some_value", "another_invalid_key": 12345}
    config = EmbeddingModelConfig(**updates)

    assert not hasattr(config, "non_existent_key")
    assert not hasattr(config, "another_invalid_key")


def test_override_default():
    user_configuration = {"openai": {"key": "user_key", "model": "user_model"}}
    config = EmbeddingModelConfig(**user_configuration)

    assert not getattr(config, "azure_openai")
    assert isinstance(config.active_config, OpenAIConfig)
    assert config.active_config.key == "user_key"
    assert config.active_config.model == "user_model"


def test_handling_both_configs():
    user_configuration = {
        "azure_openai": {"key": "azure_key"},
        "openai": {"key": "openai_key"},
    }
    config = EmbeddingModelConfig(**user_configuration)
    assert isinstance(config.active_config, AzureOpenAIConfig)
    assert config.active_config.key == "azure_key"
    assert getattr(config, "openai", None) is not None
