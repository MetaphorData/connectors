from metaphor.common.base_config import OutputConfig
from metaphor.monday.config import MondayRunConfig


def test_config(test_root_dir):
    config = MondayRunConfig.from_yaml_file(f"{test_root_dir}/monday/config.yml")

    assert config == MondayRunConfig(
        monday_api_key="monday_api_key",
        monday_api_version="monday_api_version",
        azure_openAI_key="azure_openAI_key",
        azure_openAI_version="azure_openAI_version",
        azure_openAI_endpoint="azure_openAI_endpoint",
        azure_openAI_model="text-embedding-3-small",
        azure_openAI_model_name="Embedding_3_small",
        include_text=True,
        output=OutputConfig(),
    )
