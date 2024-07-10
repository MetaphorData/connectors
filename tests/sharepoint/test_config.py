from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.sharepoint.config import SharepointRunConfig


def test_config(test_root_dir):
    config = SharepointRunConfig.from_yaml_file(
        f"{test_root_dir}/sharepoint/config.yml"
    )

    extras = {
        "azure_openAI_key": "azure_openAI_key",
        "azure_openAI_endpoint": "azure_openAI_endpoint",
    }

    embed_config = EmbeddingModelConfig()
    embed_config.update(extras)

    assert config == SharepointRunConfig(
        sharepoint_client_id="sharepoint_client_id",
        sharepoint_client_secret="sharepoint_client_secret",
        sharepoint_tenant_id="sharepoint_tenant_id",
        embed_model_config=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
