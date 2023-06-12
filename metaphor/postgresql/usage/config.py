from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.redshift.config import PostgreSQLRunConfig


@dataclass(config=ConnectorConfig)
class PostgreSQLUsageRunConfig(PostgreSQLRunConfig):
    pass
