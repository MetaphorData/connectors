from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.postgresql.profile.config import PostgreSQLProfileRunConfig


@deserialize
@dataclass
class RedshiftProfileRunConfig(PostgreSQLProfileRunConfig):
    port: int = 5439
