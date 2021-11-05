from dataclasses import dataclass, field
from typing import Set

from serde import deserialize

from metaphor.snowflake.config import SnowflakeRunConfig

DEFAULT_BATCH_SIZE = 100000


@deserialize
@dataclass
class SnowflakeUsageRunConfig(SnowflakeRunConfig):

    # Number of days back in the query log to process
    lookback_days: int = 30

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # The number of access logs fetched in a batch, default to 100000
    batch_size: int = DEFAULT_BATCH_SIZE
