from typing import Optional

from pydantic import validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class ThoughtspotRunConfig(BaseConfig):
    user: str

    # ThoughtSpot instance url
    base_url: str

    secret_key: Optional[str] = None
    password: Optional[str] = None

    @validator("password")
    def check_password_or_secret_key(cls, password, values):
        if "secret_key" not in values and not password:
            raise ValueError("Either password or secret_key is required")
        return password
