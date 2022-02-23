from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class MetabaseRunConfig(BaseConfig):
    server_url: str
    username: str
    password: str
