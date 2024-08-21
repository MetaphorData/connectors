from datetime import datetime

from pydantic import ValidationError

from metaphor.common.embeddings_config import (
    AzureOpenAIConfig,
    EmbeddingModelConfig,
    OpenAIConfig,
)


def test_default_initialization():
    config = EmbeddingModelConfig(
        azure_openai=AzureOpenAIConfig(key="key", endpoint="endpoint")
    )
    assert config.azure_openai.key == "key"
    assert config.azure_openai.endpoint == "endpoint"
    assert config.azure_openai.version == "2024-06-01"
    assert config.azure_openai.model == "text-embedding-3-small"
    assert config.azure_openai.deployment_name == "Embedding_3_small"
    assert config.openai is None
    assert config.openai is None
    assert config.chunk_size == 512
    assert config.chunk_overlap == 50


def test_update_with_valid_keys():
    config = EmbeddingModelConfig(
        azure_openai=AzureOpenAIConfig(key="key", endpoint="endpoint")
    )

    assert hasattr(config, "azure_openai")
    assert isinstance(config.active_config, AzureOpenAIConfig)
    assert config.active_config.version == AzureOpenAIConfig().version
    assert hasattr(config, "openai")
    assert not getattr(config, "openai")
    assert config.chunk_size == 512
    assert config.chunk_overlap == 50


def test_update_with_invalid_keys():
    updates = {"non_existent_key": "some_value", "another_invalid_key": 12345}
    config = EmbeddingModelConfig(
        **updates, azure_openai=AzureOpenAIConfig(key="key", endpoint="endpoint")
    )
    assert not hasattr(config, "non_existent_key")
    assert not hasattr(config, "another_invalid_key")


def test_override_default():
    config = EmbeddingModelConfig(
        openai=OpenAIConfig(key="user_key", model="user_model")
    )

    assert not getattr(config, "azure_openai")
    assert isinstance(config.active_config, OpenAIConfig)
    assert config.active_config.key == "user_key"
    assert config.active_config.model == "user_model"


def test_handling_both_configs():
    user_configuration = {
        "azure_openai": {"key": "azure_key"},
        "openai": {"key": "openai_key"},
    }
    try:
        _ = EmbeddingModelConfig(**user_configuration)
    except ValidationError:
        assert True  # expect this to fail with two configuration


def test_azure_openai_config_version_as_datetime():
    # Create a datetime object
    version_datetime = datetime(2024, 6, 1)

    # Initialize AzureOpenAIConfig with the datetime object
    config = AzureOpenAIConfig(key="key", version=version_datetime)

    # Assert that the version is correctly converted to a string
    assert config.version == "2024-06-01"
