from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.common.base_config import BaseConfig


@deserialize
@dataclass
class MetabaseRunConfig(BaseConfig):
    server_url: str
    username: str
    password: str
