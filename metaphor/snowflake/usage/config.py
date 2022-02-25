from dataclasses import field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.snowflake.config import SnowflakeRunConfig

DEFAULT_BATCH_SIZE = 100000


@dataclass
class SnowflakeUsageRunConfig(SnowflakeRunConfig):
    # whether to write to dataset usage history or dataset usage aspect
    use_history: bool = True

    # Number of days back in the query log to process if use_history = False, otherwise, it's locked to 1 day
    lookback_days: int = 30

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # The number of access logs fetched in a batch, default to 100000
    batch_size: int = DEFAULT_BATCH_SIZE
