from dataclasses import field as dataclass_field
from typing import Any, Dict, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter


@dataclass(config=ConnectorConfig)
class HiveRunConfig(BaseConfig):
    host: str
    port: int
    auth: Optional[str] = None
    password: Optional[str] = None

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = dataclass_field(default_factory=lambda: DatasetFilter())

    @property
    def connect_kwargs(self) -> Dict[str, Any]:
        kwargs = {
            "host": self.host,
            "port": self.port,
        }
        if self.auth:
            kwargs["auth"] = self.auth
        if self.password:
            kwargs["password"] = self.password
        return kwargs
