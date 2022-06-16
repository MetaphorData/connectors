from dataclasses import field as dataclass_field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.snowflake.config import SnowflakeRunConfig

DEFAULT_BATCH_SIZE = 100000


@dataclass
class SnowflakeQueryRunConfig(SnowflakeRunConfig):
    # The number of days' logs to fetch
    lookback_days: int = 30

    # The number of access logs fetched in a batch, default to 100000
    batch_size: int = DEFAULT_BATCH_SIZE

    # Maximum number of recent queries to capture for each table
    max_queries_per_table: int = 100

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())
