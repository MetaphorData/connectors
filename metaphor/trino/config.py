from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter


@dataclass(config=ConnectorConfig)
class TrinoRunConfig(BaseConfig):
    host: str
    port: int
    username: str
    password: Optional[str] = None
    token: Optional[str] = None

    enable_tls: bool = False

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())
