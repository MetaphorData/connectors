from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.monday.config import MondayRunConfig


def test_config(test_root_dir):
    config = MondayRunConfig.from_yaml_file(f"{test_root_dir}/monday/config.yml")

    extras = {
        "azure_openAI_key": "azure_openAI_key",
        "azure_openAI_endpoint": "azure_openAI_endpoint",
    }

    embed_config = EmbeddingModelConfig()
    embed_config.update(extras)

    assert config == MondayRunConfig(
        monday_api_key="monday_api_key",
        monday_api_version="monday_api_version",
        embed_model_config=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
