from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import AzureOpenAIConfig, EmbeddingModelConfig
from metaphor.confluence.config import ConfluenceRunConfig


def test_config(test_root_dir):
    config = ConfluenceRunConfig.from_yaml_file(
        f"{test_root_dir}/confluence/config.yml"
    )

    embed_config = EmbeddingModelConfig(
        AzureOpenAIConfig(key="azure_openAI_key", endpoint="azure_openAI_endpoint")
    )

    print(config.embedding_model)
    print("\n\n")
    print(embed_config)

    assert config == ConfluenceRunConfig(
        confluence_base_URL="https://test.atlassian.net/wiki",
        confluence_cloud=True,
        select_method="label",
        confluence_username="test@metaphor.io",
        confluence_token="token",
        space_key="KB",
        embedding_model=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
