from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import AzureOpenAIConfig, EmbeddingModelConfig
from metaphor.static_web.config import StaticWebRunConfig


def test_config(test_root_dir):
    config = StaticWebRunConfig.from_yaml_file(f"{test_root_dir}/static_web/config.yml")

    embed_config = EmbeddingModelConfig(
        AzureOpenAIConfig(key="azure_openAI_key", endpoint="azure_openAI_endpoint")
    )

    assert config == StaticWebRunConfig(
        links=["https://metaphor.io/"],
        depths=[1],
        embedding_model=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
