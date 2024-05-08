from metaphor.common.base_config import OutputConfig
from metaphor.sharepoint.config import SharepointRunConfig


def test_config(test_root_dir):
    config = SharepointRunConfig.from_yaml_file(
        f"{test_root_dir}/sharepoint/config.yml"
    )

    assert config == SharepointRunConfig(
        sharepoint_client_id="sharepoint_client_id",
        sharepoint_client_secret="sharepoint_client_secret",
        sharepoint_tenant_id="sharepoint_tenant_id",
        azure_openAI_key="azure_openAI_key",
        azure_openAI_version="azure_openAI_version",
        azure_openAI_endpoint="azure_openAI_endpoint",
        azure_openAI_model="text-embedding-3-small",
        azure_openAI_model_name="Embedding_3_small",
        include_text=True,
        output=OutputConfig(),
    )
