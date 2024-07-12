from metaphor.common.base_config import OutputConfig
from metaphor.common.embeddings_config import EmbeddingModelConfig
from metaphor.sharepoint.config import SharepointRunConfig


def test_config(test_root_dir):
    config = SharepointRunConfig.from_yaml_file(
        f"{test_root_dir}/sharepoint/config.yml"
    )

    test_config = {
        "azure_openai": {"key": "azure_openAI_key", "endpoint": "azure_openAI_endpoint"}
    }
    embed_config = EmbeddingModelConfig(**test_config)

    assert config == SharepointRunConfig(
        sharepoint_client_id="sharepoint_client_id",
        sharepoint_client_secret="sharepoint_client_secret",
        sharepoint_tenant_id="sharepoint_tenant_id",
        embedding_model=embed_config,
        include_text=True,
        output=OutputConfig(),
    )
