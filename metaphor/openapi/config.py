from typing import List, Optional

from pydantic import FilePath, HttpUrl, model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.utils import must_set_at_least_one, must_set_exactly_one


@dataclass(config=ConnectorConfig)
class BasicAuth:
    user: str
    password: str


@dataclass(config=ConnectorConfig)
class OpenAPIAuthConfig:
    basic_auth: Optional[BasicAuth] = None


@dataclass(config=ConnectorConfig)
class OpenAPIJsonConfig:
    base_url: HttpUrl
    openapi_json_path: Optional[FilePath] = None
    openapi_json_url: Optional[HttpUrl] = None
    auth: Optional[OpenAPIAuthConfig] = None

    @model_validator(mode="after")
    def have_path_or_url(self) -> "OpenAPIJsonConfig":
        must_set_exactly_one(self.__dict__, ["openapi_json_path", "openapi_json_url"])
        return self


@dataclass(config=ConnectorConfig)
class OpenAPIRunConfig(BaseConfig):
    base_url: Optional[HttpUrl] = None
    openapi_json_path: Optional[FilePath] = None
    openapi_json_url: Optional[HttpUrl] = None
    auth: Optional[OpenAPIAuthConfig] = None
    specs: Optional[List[OpenAPIJsonConfig]] = None

    @model_validator(mode="after")
    def must_have_at_least_one_spec_config(self) -> "OpenAPIRunConfig":
        must_set_at_least_one(self.__dict__, ["specs", "base_url"])

        if self.__dict__.get("base_url") is None and self.__dict__.get("specs"):
            if not self.specs:
                raise ValueError("Must have at least one spec")

        if self.__dict__.get("base_url"):
            must_set_exactly_one(
                self.__dict__, ["openapi_json_path", "openapi_json_url"]
            )
        return self
