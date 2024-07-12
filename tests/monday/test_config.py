from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.monday.config import MondayRunConfig


def test_config(test_root_dir):
    config = MondayRunConfig.from_yaml_file(f"{test_root_dir}/monday/config.yml")

    test_config = {
        "azure_openai": {"key": "azure_openAI_key", "endpoint": "azure_openAI_endpoint"}
    }
    embed_config = EmbeddingModelConfig(**test_config)

    assert config == MondayRunConfig(
        monday_api_key="monday_api_key",
        monday_api_version="monday_api_version",
        embedding_model=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
