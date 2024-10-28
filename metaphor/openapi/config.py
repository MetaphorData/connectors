from typing import Optional

from pydantic import FilePath, HttpUrl, model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.utils import must_set_exactly_one


@dataclass(config=ConnectorConfig)
class BasicAuth:
    user: str
    password: str


@dataclass(config=ConnectorConfig)
class OpenAPIAuthConfig:
    basic_auth: Optional[BasicAuth] = None


@dataclass(config=ConnectorConfig)
class OpenAPIRunConfig(BaseConfig):
    base_url: HttpUrl
    openapi_json_path: Optional[FilePath] = None
    openapi_json_url: Optional[HttpUrl] = None
    auth: Optional[OpenAPIAuthConfig] = None

    @model_validator(mode="after")
    def have_path_or_url(self) -> "OpenAPIRunConfig":
        must_set_exactly_one(self.__dict__, ["openapi_json_path", "openapi_json_url"])
        return self
