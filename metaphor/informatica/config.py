from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class InformaticaRunConfig(BaseConfig):
    user: str

    password: str

    base_url: str
