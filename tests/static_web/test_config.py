from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.static_web.config import StaticWebRunConfig


def test_config(test_root_dir):
    config = StaticWebRunConfig.from_yaml_file(f"{test_root_dir}/static_web/config.yml")

    extras = {
        "azure_openAI_key": "azure_openAI_key",
        "azure_openAI_endpoint": "azure_openAI_endpoint",
    }

    embed_config = EmbeddingModelConfig()
    embed_config.update(extras)

    assert config == StaticWebRunConfig(
        links=["https://metaphor.io/"],
        depths=[1],
        embed_model_config=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
