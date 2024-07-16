from typing import Any, Optional

from pydantic import BaseModel, model_validator

from metaphor.common.utils import must_set_exactly_one

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

    @model_validator(mode="after")
    def check_only_one_config(self):
        """
        Ensure that only one of `supported_sources` is set.
        """
        must_set_exactly_one(self.__dict__, supported_sources)
        return self

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
