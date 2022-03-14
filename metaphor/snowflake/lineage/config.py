from pydantic.dataclasses import dataclass

from metaphor.snowflake.config import SnowflakeRunConfig

DEFAULT_BATCH_SIZE = 100000


@dataclass
class SnowflakeLineageRunConfig(SnowflakeRunConfig):
    # Number of days back in the query log to process
    lookback_days: int = 30

    # The number of access logs fetched in a batch, default to 100000
    batch_size: int = DEFAULT_BATCH_SIZE
