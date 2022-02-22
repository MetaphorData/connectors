from pydantic.dataclasses import dataclass

from metaphor.postgresql.profile.config import PostgreSQLProfileRunConfig


@dataclass
class RedshiftProfileRunConfig(PostgreSQLProfileRunConfig):
    port: int = 5439
