from dataclasses import dataclass

from serde import deserialize

from metaphor.common.extractor import RunConfig


@deserialize
@dataclass
class PostgreSQLRunConfig(RunConfig):
    host: str
    database: str
    user: str
    password: str

    port: int = 5432
