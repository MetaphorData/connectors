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

    # Deprecated: Not used. Will be dropped in 0.15
    max_results: Optional[int] = 1000


@dataclass(config=ConnectorConfig)
class UnityCatalogRunConfig(BaseConfig):
    # cluster/warehouse hostname & HTTP path
    hostname: str
    http_path: str

    # API token
    token: str

    # Override the URL for each dataset
    source_url: Optional[str] = None

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # configs for fetching query logs
    query_log: UnityCatalogQueryLogConfig = field(
        default_factory=lambda: UnityCatalogQueryLogConfig()
    )

    # Max number of connection to the database
    max_concurrency: int = 10

    # The limit to apply when running DESCRIBE HISTORY
    describe_history_limit: int = 100
