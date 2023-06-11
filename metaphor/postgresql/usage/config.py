from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import DataclassConfig
from metaphor.redshift.config import PostgreSQLRunConfig


@dataclass(config=DataclassConfig)
class PostgreSQLUsageRunConfig(PostgreSQLRunConfig):
    pass
