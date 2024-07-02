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
class MetabaseAssetFilter:
    includes: List[str] = field(default_factory=list)
    excludes: List[str] = field(default_factory=list)

    def include_path(self, collection_path: List[str]):
        path = "/".join(collection_path)
        if self.includes and not any(path.startswith(item) for item in self.includes):
            return False
        if self.excludes and any(path.startswith(item) for item in self.excludes):
            return False
        return True


@dataclass(config=ConnectorConfig)
class MetabaseRunConfig(BaseConfig):
    server_url: str
    username: str
    password: str

    database_defaults: List[MetabaseDatabaseDefaults] = field(default_factory=list)

    filter: MetabaseAssetFilter = field(default_factory=MetabaseAssetFilter)
