from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter


@dataclass(config=ConnectorConfig)
class UnityCatalogRunConfig(BaseConfig):
    host: str
    token: str

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())
