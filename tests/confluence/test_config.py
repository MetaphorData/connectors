import pytest
from pydantic import ValidationError

from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.confluence.config import ConfluenceRunConfig

azure_config = {
    "azure_openai": {"key": "azure_openAI_key", "endpoint": "azure_openAI_endpoint"}
}
embed_config = EmbeddingModelConfig(**azure_config)


def test_config(test_root_dir):
    config = ConfluenceRunConfig.from_yaml_file(
        f"{test_root_dir}/confluence/config.yml"
    )

    assert config == ConfluenceRunConfig(
        confluence_base_URL="https://test.atlassian.net/wiki",
        confluence_cloud=True,
        confluence_username="test@metaphor.io",
        confluence_token="token",
        space_keys=["KB", "KB2"],
        embedding_model=embed_config,
        include_text=True,
        output=OutputConfig(),
    )


def test_config_validation():
    # Test valid cloud configuration
    valid_cloud_config = ConfluenceRunConfig(
        confluence_base_URL="https://test.atlassian.net/wiki",
        confluence_cloud=True,
        confluence_username="test@metaphor.io",
        confluence_token="token",
        space_keys=["KB"],
        embedding_model=embed_config,
        output=OutputConfig(),
    )
    assert valid_cloud_config is not None

    # Test invalid cloud configuration (missing username)
    with pytest.raises(
        ValueError, match="confluence_username and confluence_token must be provided"
    ):
        ConfluenceRunConfig(
            confluence_base_URL="https://test.atlassian.net/wiki",
            confluence_cloud=True,
            confluence_token="token",
            space_keys=["KB"],
            embedding_model=embed_config,
            output=OutputConfig(),
        )

    # Test valid server configuration
    valid_server_config = ConfluenceRunConfig(
        confluence_base_URL="https://test.atlassian.net/wiki",
        confluence_cloud=False,
        confluence_PAT="pat_token",
        space_keys=["KB"],
        embedding_model=embed_config,
        output=OutputConfig(),
    )
    assert valid_server_config is not None

    # Test invalid server configuration (missing PAT)
    with pytest.raises(ValueError, match="confluence_PAT must be provided"):
        ConfluenceRunConfig(
            confluence_base_URL="https://test.atlassian.net/wiki",
            confluence_cloud=False,
            space_keys=["KB"],
            embedding_model=embed_config,
            output=OutputConfig(),
        )

    # Test invalid selection method (multiple selection methods)
    with pytest.raises((ValidationError)):
        ConfluenceRunConfig(
            confluence_base_URL="https://test.atlassian.net/wiki",
            confluence_cloud=True,
            confluence_username="test@metaphor.io",
            confluence_token="token",
            space_keys=["KB"],
            page_ids=["123"],
            embedding_model=embed_config,
            output=OutputConfig(),
        )

    # Test invalid selection method (no selection method)
    with pytest.raises(ValidationError):
        ConfluenceRunConfig(
            confluence_base_URL="https://test.atlassian.net/wiki",
            confluence_cloud=True,
            confluence_username="test@metaphor.io",
            confluence_token="token",
            embedding_model=embed_config,
            output=OutputConfig(),
        )
