from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import TwoLevelDatasetFilter


@dataclass(config=ConnectorConfig)
class GenericDatabaseRunConfig(BaseConfig):
    host: str  # host of the database the crawler will connect to
    database: str
    user: str
    password: str

    # Alternative hostname to build the entity logical ID
    alternative_host: Optional[str] = None

    # Include or exclude specific databases/schemas/tables
    filter: TwoLevelDatasetFilter = field(
        default_factory=lambda: TwoLevelDatasetFilter()
    )

    port: int = 3306  # Use MySQL port here, subclass should overwrite this
