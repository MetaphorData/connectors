from metaphor.common.base_config import OutputConfig
from metaphor.static_web.config import StaticWebRunConfig


def test_config(test_root_dir):
    config = StaticWebRunConfig.from_yaml_file(f"{test_root_dir}/static_web/config.yml")

    assert config == StaticWebRunConfig(
        links=["https://metaphor.io/"],
        depths=[1],
        azure_openAI_key="azure_openAI_key",
        azure_openAI_version="azure_openAI_version",
        azure_openAI_endpoint="azure_openAI_endpoint",
        azure_openAI_model="text-embedding-ada-002",
        azure_openAI_model_name="EmbeddingModel",
        include_text=True,
        output=OutputConfig(),
    )
