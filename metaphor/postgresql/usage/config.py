from pydantic.dataclasses import dataclass

from metaphor.redshift.config import PostgreSQLRunConfig


@dataclass
class PostgreSQLUsageRunConfig(PostgreSQLRunConfig):
    pass
