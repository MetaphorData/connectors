from dataclasses import dataclass

from serde import deserialize

from metaphor.postgresql.config import PostgreSQLRunConfig


@deserialize
@dataclass
class RedshiftSQLRunConfig(PostgreSQLRunConfig):
    port: int = 5439
