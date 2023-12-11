from metaphor.common.base_config import OutputConfig
from metaphor.notion.config import NotionRunConfig


def test_config(test_root_dir):
    config = NotionRunConfig.from_yaml_file(f"{test_root_dir}/notion/config.yml")

    assert config == NotionRunConfig(
        notion_api_tok="notion_api_tok",
        azure_openAI_key="azure_openAI_key",
        azure_openAI_ver="azure_openAI_ver",
        azure_openAI_endpoint="azure_openAI_endpoint",
        azure_openAI_model="text-embedding-ada-002",
        azure_openAI_model_name="EmbeddingModel",
        output=OutputConfig(),
    )
