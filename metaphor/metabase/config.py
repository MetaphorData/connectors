from dataclasses import field
from typing import List, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass
class MetabaseDatabaseDefaults:
    id: int
    default_schema: Optional[str] = None


@dataclass(config=ConnectorConfig)
class MetabaseRunConfig(BaseConfig):
    server_url: str
    username: str
    password: str

    database_defaults: List[MetabaseDatabaseDefaults] = field(default_factory=list)
