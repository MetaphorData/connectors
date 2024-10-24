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
    base_url: str
    openapi_json_path: str

    api_name: Optional[str] = None

    auth: Optional[OpenAPIAuthConfig] = None
