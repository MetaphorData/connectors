from dataclasses import field
from typing import Literal, Optional

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

    http_scheme: Optional[Literal["https"]] = None

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())
