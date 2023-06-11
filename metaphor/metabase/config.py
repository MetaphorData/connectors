from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import DataclassConfig


@dataclass(config=DataclassConfig)
class MetabaseRunConfig(BaseConfig):
    server_url: str
    username: str
    password: str
