from dataclasses import dataclass, field
from typing import Optional

from serde import deserialize

from metaphor.common.extractor import RunConfig
from metaphor.common.filter import DatasetFilter


@deserialize
@dataclass
class PostgreSQLRunConfig(RunConfig):
    host: str
    database: str
    user: str
    password: str

    # Include or exclude specific databases/schemas/tables
    filter: Optional[DatasetFilter] = field(default_factory=lambda: DatasetFilter())

    port: int = 5432
