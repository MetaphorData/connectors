from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.postgresql.config import BasePostgreSQLRunConfig


@dataclass(config=ConnectorConfig)
class PostgreSQLUsageRunConfig(BasePostgreSQLRunConfig):
    pass
