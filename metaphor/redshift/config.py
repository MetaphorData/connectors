from pydantic.dataclasses import dataclass

from metaphor.postgresql.config import PostgreSQLRunConfig


@dataclass
class RedshiftRunConfig(PostgreSQLRunConfig):
    port: int = 5439
