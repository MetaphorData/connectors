from dataclasses import dataclass


@dataclass
class EmbeddingModelConfig:
    """
    Config fields and defaults for supported embeddings services.
    """

    # Azure OpenAI services
    azure_openAI_key: str = ""
    azure_openAI_endpoint: str = ""
    azure_openAI_version: str = "2024-03-01-preview"
    azure_openAI_model: str = "text-embedding-3-small"
    azure_openAI_model_name: str = "Embedding_3_small"

    # OpenAI
    openAI_key: str = ""
    openAI_model: str = "text-embedding-3-small"

    # Chunk & overlap size
    chunk_size: int = 512
    chunk_overlap_size: int = 50

    def update(self, config_dict):
        for key, value in config_dict.items():
            if not hasattr(self, key):  # check if attribute doesn't exist
                setattr(self, key, value)  # set with defaults
