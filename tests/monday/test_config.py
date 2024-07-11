from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import AzureOpenAIConfig, EmbeddingModelConfig
from metaphor.monday.config import MondayRunConfig


def test_config(test_root_dir):
    config = MondayRunConfig.from_yaml_file(f"{test_root_dir}/monday/config.yml")

    embed_config = EmbeddingModelConfig(
        AzureOpenAIConfig(key="azure_openAI_key", endpoint="azure_openAI_endpoint")
    )

    assert config == MondayRunConfig(
        monday_api_key="monday_api_key",
        monday_api_version="monday_api_version",
        embedding_model=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
