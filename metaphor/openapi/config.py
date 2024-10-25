from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class BasicAuth:
    user: str
    password: str


@dataclass(config=ConnectorConfig)
class OpenAPIAuthConfig:
    basic_auth: Optional[BasicAuth] = None


@dataclass(config=ConnectorConfig)
class OpenAPIRunConfig(BaseConfig):
    openapi_json_path: str  # URL or file path
    base_url: str  # base_url of endpoints

    auth: Optional[OpenAPIAuthConfig] = None
