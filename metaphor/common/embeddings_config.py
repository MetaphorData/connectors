from dataclasses import dataclass, field
from typing import Dict

supported_sources = ["azure_openai", "openai"]


@dataclass
class AzureOpenAIConfig:
    key: str = ""
    endpoint: str = ""
    version: str = "2024-03-01-preview"
    model: str = "text-embedding-3-small"
    model_name: str = "Embedding_3_small"


@dataclass
class OpenAIConfig:
    key: str = ""
    model: str = "text-embedding-3-small"


@dataclass
class EmbeddingModelConfig:
    azure_openai: AzureOpenAIConfig = field(default_factory=AzureOpenAIConfig)
    openai: OpenAIConfig = field(default_factory=OpenAIConfig)
    chunk_size: int = 512
    chunk_overlap: int = 50

    def update(self, config_dict: Dict[str, Dict]):
        """
        Method to update config with some incoming configuration.
        Checks that the incoming configuration is supported and the right type.
        In the connectors, the "incoming config" is the default config,
            which is why previously-set attributes should not be overwritten.
        """
        for key, value in config_dict.items():
            if key in supported_sources and isinstance(value, dict):
                config_obj = getattr(self, key, None)
                if getattr(
                    config_obj, "key", None
                ):  # Only update defaults for a configured source
                    for sub_key, sub_value in value.items():
                        if hasattr(
                            config_obj, sub_key
                        ) and not getattr(  # If the key exists but is not set
                            config_obj, sub_key
                        ):
                            setattr(config_obj, sub_key, sub_value)
            elif hasattr(self, key) and not getattr(self, key):
                setattr(self, key, value)
