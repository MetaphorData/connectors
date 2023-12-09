from dataclasses import field
from typing import Optional, Set

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter


@dataclass(config=ConnectorConfig)
class UnityCatalogQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # Limit the number of results returned in one page. The default is 100.
    max_results: Optional[int] = 100


@dataclass(config=ConnectorConfig)
class UnityCatalogRunConfig(BaseConfig):
    host: str
    token: str

    # Override the URL for each dataset
    source_url: Optional[str] = None

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # configs for fetching query logs
    query_log: UnityCatalogQueryLogConfig = UnityCatalogQueryLogConfig()
