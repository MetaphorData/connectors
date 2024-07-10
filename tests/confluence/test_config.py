from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.confluence.config import ConfluenceRunConfig


def test_config(test_root_dir):
    config = ConfluenceRunConfig.from_yaml_file(
        f"{test_root_dir}/confluence/config.yml"
    )

    extras = {
        "azure_openAI_key": "azure_openAI_key",
        "azure_openAI_endpoint": "azure_openAI_endpoint",
    }

    embed_config = EmbeddingModelConfig()
    embed_config.update(extras)

    assert config == ConfluenceRunConfig(
        confluence_base_URL="https://test.atlassian.net/wiki",
        confluence_cloud=True,
        select_method="label",
        confluence_username="test@metaphor.io",
        confluence_token="token",
        space_key="KB",
        embed_model_config=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
