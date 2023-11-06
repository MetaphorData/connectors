from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig

@dataclass(config=ConnectorConfig)
class NotionRunConfig(BaseConfig):
    # integration authorization token
    api_key_token: str

    # API version - built for 2022-06-28 typically
    api_key_version: str