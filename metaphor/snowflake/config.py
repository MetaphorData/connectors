from dataclasses import field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.auth import SnowflakeAuthConfig
from metaphor.snowflake.utils import DEFAULT_THREAD_POOL_SIZE

# number of query logs to fetch from Snowflake in one batch
DEFAULT_QUERY_LOG_FETCH_SIZE = 100000


@dataclass
class SnowflakeQueryLogConfig:

    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # The number of query logs to fetch from Snowflake in one batch
    fetch_size: int = DEFAULT_QUERY_LOG_FETCH_SIZE


@dataclass
class SnowflakeRunConfig(SnowflakeAuthConfig):

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # Max number of concurrent queries to database
    max_concurrency: int = DEFAULT_THREAD_POOL_SIZE

    # configs for fetching query logs
    query_log: SnowflakeQueryLogConfig = SnowflakeQueryLogConfig()
