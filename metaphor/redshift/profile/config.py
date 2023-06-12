from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.postgresql.profile.config import PostgreSQLProfileRunConfig


@dataclass(config=ConnectorConfig)
class RedshiftProfileRunConfig(PostgreSQLProfileRunConfig):
    port: int = 5439
