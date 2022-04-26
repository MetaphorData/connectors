from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class ThoughtspotRunConfig(BaseConfig):
    user: str

    # ThoughtSpot instance url
    base_url: str

    password: Optional[str] = None
    secret_key: Optional[str] = None
