from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import DataclassConfig
from metaphor.postgresql.profile.config import PostgreSQLProfileRunConfig


@dataclass(config=DataclassConfig)
class RedshiftProfileRunConfig(PostgreSQLProfileRunConfig):
    port: int = 5439
