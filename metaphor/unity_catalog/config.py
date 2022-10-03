from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class UnityCatalogRunConfig(BaseConfig):
    host: str
    token: str
