from metaphor.common.base_config import OutputConfig
from metaphor.confluence.config import ConfluenceRunConfig

def test_config(test_root_dir):
    config = ConfluenceRunConfig.from_yaml_file(f"{test_root_dir}/confluence/config.yml")

    assert config == ConfluenceRunConfig(
        confluence_base_URL="https://test.atlassian.net/wiki",
        confluence_cloud=True,
        select_method = "label",
        confluence_username = "test@metaphor.io",
        confluence_token = "token",
        space_key = "KB",
        azure_openAI_key="azure_openAI_key",
        azure_openAI_version="azure_openAI_version",
        azure_openAI_endpoint="azure_openAI_endpoint",
        azure_openAI_model="text-embedding-ada-002",
        azure_openAI_model_name="Embedding_ada002",
        include_text=True,
        output=OutputConfig(),
    )
    