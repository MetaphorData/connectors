from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import DataclassConfig
from metaphor.common.filter import TwoLevelDatasetFilter


@dataclass(config=DataclassConfig)
class MySQLRunConfig(BaseConfig):
    host: str
    database: str
    user: str
    password: str

    # Include or exclude specific databases/schemas/tables
    filter: TwoLevelDatasetFilter = field(
        default_factory=lambda: TwoLevelDatasetFilter()
    )

    port: int = 5432
