from dataclasses import dataclass

from serde import deserialize

from metaphor.postgresql.config import PostgreSQLRunConfig


@deserialize
@dataclass
class RedshiftRunConfig(PostgreSQLRunConfig):
    port: int = 5439
