from typing import Optional

from pydantic import model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.utils import must_set_at_least_one


@dataclass(config=ConnectorConfig)
class ThoughtSpotRunConfig(BaseConfig):
    user: str

    # ThoughtSpot instance url
    base_url: str

    secret_key: Optional[str] = None
    password: Optional[str] = None

    @model_validator(mode="after")
    def check_password_or_secret_key(self) -> "ThoughtSpotRunConfig":
        must_set_at_least_one(self.__dict__, ["secret_key", "password"])
        return self
