from typing import Any, Dict, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class HiveRunConfig(BaseConfig):
    host: str
    port: int
    auth_user: Optional[str] = None
    password: Optional[str] = None

    collect_stats: bool = False

    @property
    def connect_kwargs(self) -> Dict[str, Any]:
        kwargs = {
            "host": self.host,
            "port": self.port,
        }
        if self.auth_user:
            kwargs["auth"] = self.auth_user
        if self.password:
            kwargs["password"] = self.password
        return kwargs
