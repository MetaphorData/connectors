from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.common.base_config import BaseConfig
from metaphor.common.filter import DatasetFilter


@deserialize
@dataclass
class PostgreSQLRunConfig(BaseConfig):
    host: str
    database: str
    user: str
    password: str

    # Include or exclude specific databases/schemas/tables
    filter: Optional[DatasetFilter] = field(default_factory=lambda: DatasetFilter())

    port: int = 5432

    # Ignore RDS Postgres system databases
    ignored_dbs: set[str] = field(
        default_factory=lambda: set(["template0", "template1", "rdsadmin"])
    )
