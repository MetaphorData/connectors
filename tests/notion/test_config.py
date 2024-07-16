from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.notion.config import NotionRunConfig


def test_config(test_root_dir):
    config = NotionRunConfig.from_yaml_file(f"{test_root_dir}/notion/config.yml")

    test_config = {
        "azure_openai": {"key": "azure_openAI_key", "endpoint": "azure_openAI_endpoint"}
    }
    embed_config = EmbeddingModelConfig(**test_config)

    assert config == NotionRunConfig(
        notion_api_token="notion_api_token",
        embedding_model=embed_config,
        include_text=True,
        notion_api_version="2022-06-08",
        output=OutputConfig(),
    )
