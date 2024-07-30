from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.database.config import GenericDatabaseRunConfig


@dataclass(config=ConnectorConfig)
class MySQLRunConfig(GenericDatabaseRunConfig):
    port: int = 3306
