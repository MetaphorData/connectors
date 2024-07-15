from typing import Any, Dict, Optional

from pydantic import BaseModel, model_validator

supported_sources = ["azure_openai", "openai"]


class AzureOpenAIConfig(BaseModel):
    key: Optional[str] = None
    endpoint: Optional[str] = None
    version: Optional[str] = "2024-03-01-preview"
    model: Optional[str] = "text-embedding-3-small"
    deployment_name: Optional[str] = "Embedding_3_small"


class OpenAIConfig(BaseModel):
    key: Optional[str] = None
    model: Optional[str] = "text-embedding-3-small"


class EmbeddingModelConfig(BaseModel):
    azure_openai: Optional[AzureOpenAIConfig] = None
    openai: Optional[OpenAIConfig] = None
    chunk_size: int = 512
    chunk_overlap: int = 50

    @model_validator(mode="before")
    def check_only_one_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure that only one of `supported_sources` is set.
        """
        config_count = sum(
            1 for key in supported_sources if values.get(key) is not None
        )
        if config_count != 1:
            raise ValueError(f"Exactly one of {supported_sources} must be set")
        return values

    @property
    def active_config(self) -> Any:
        """
        Returns the first valid configuration.
        """
        for key in supported_sources:
            config_obj = getattr(self, key, None)
            if config_obj and config_obj.key:
                return config_obj
        raise ValueError("No valid embedding model configuration found")
