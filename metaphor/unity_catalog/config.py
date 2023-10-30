from dataclasses import field
from typing import List, Optional

from databricks.sdk.service.sql import TimeRange
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter


@dataclass(config=ConnectorConfig)
class UnityCatalogQueryLogConfig:
    # Filter for querying logs. No filter if not specified.
    query_start_time_range: Optional[TimeRange] = None

    # Query log filter to match certain usernames
    user_ids: List[int] = field(default_factory=lambda: [])


@dataclass(config=ConnectorConfig)
class UnityCatalogRunConfig(BaseConfig):
    host: str
    token: str

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # configs for fetching query logs
    query_log: UnityCatalogQueryLogConfig = UnityCatalogQueryLogConfig()
