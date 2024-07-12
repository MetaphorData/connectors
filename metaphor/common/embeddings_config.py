from typing import Any, Dict, Optional

from pydantic import BaseModel, root_validator

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

    @root_validator(pre=True)
    def apply_defaults(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Applies defaults to the first non-None provided configuration.
        """
        for key in supported_sources:
            provided_config = values.get(key)
            if provided_config:
                if key == "azure_openai":
                    default_config = AzureOpenAIConfig()
                    updated_config = default_config.model_copy(update=provided_config)
                    values[key] = updated_config
                elif key == "openai":
                    default_config = OpenAIConfig()  # type: ignore
                    updated_config = default_config.model_copy(update=provided_config)
                    values[key] = updated_config
                else:
                    raise ValueError("No valid embedding model configuration found")
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
