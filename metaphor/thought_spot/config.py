from typing import Optional

from pydantic import model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class ThoughtSpotRunConfig(BaseConfig):
    user: str

    # ThoughtSpot instance url
    base_url: str

    secret_key: Optional[str] = None
    password: Optional[str] = None

    @model_validator(mode="after")
    def check_password_or_secret_key(self) -> "ThoughtSpotRunConfig":
        if self.password is None and self.secret_key is None:
            raise ValueError("Either password or secret_key is required")
        return self
