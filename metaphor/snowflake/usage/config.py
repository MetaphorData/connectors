from dataclasses import dataclass, field
from typing import Set

from serde import deserialize

from metaphor.snowflake.config import SnowflakeRunConfig


@deserialize
@dataclass
class SnowflakeUsageRunConfig(SnowflakeRunConfig):
    # The number of days in history to retrieve query log
    lookback_days: int = 30

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = field(default_factory=lambda: set())
