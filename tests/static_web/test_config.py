from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.static_web.config import StaticWebRunConfig


def test_config(test_root_dir):
    config = StaticWebRunConfig.from_yaml_file(f"{test_root_dir}/static_web/config.yml")

    test_config = {
        "azure_openai": {"key": "azure_openAI_key", "endpoint": "azure_openAI_endpoint"}
    }
    embed_config = EmbeddingModelConfig(**test_config)

    assert config == StaticWebRunConfig(
        links=["https://metaphor.io/"],
        depths=[1],
        embedding_model=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
