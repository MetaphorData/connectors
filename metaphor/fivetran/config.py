from dataclasses import field as dataclass_field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter


@dataclass(config=ConnectorConfig)
class FivetranRunConfig(BaseConfig):
    api_key: str
    api_secret: str

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = dataclass_field(default_factory=lambda: DatasetFilter())
