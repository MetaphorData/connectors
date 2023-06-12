from dataclasses import field
from typing import List, Set

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.tag_matcher import TagMatcher
from metaphor.snowflake.auth import SnowflakeAuthConfig
from metaphor.snowflake.utils import DEFAULT_THREAD_POOL_SIZE

# number of query logs to fetch from Snowflake in one batch
DEFAULT_QUERY_LOG_FETCH_SIZE = 100000


@dataclass(config=ConnectorConfig)
class SnowflakeQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # The number of query logs to fetch from Snowflake in one batch
    fetch_size: int = DEFAULT_QUERY_LOG_FETCH_SIZE


@dataclass(config=ConnectorConfig)
class SnowflakeRunConfig(SnowflakeAuthConfig):
    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # Max number of concurrent queries to database
    max_concurrency: int = DEFAULT_THREAD_POOL_SIZE

    # How tags should be assigned to datasets
    tag_matchers: List[TagMatcher] = field(default_factory=lambda: [])

    # configs for fetching query logs
    query_log: SnowflakeQueryLogConfig = SnowflakeQueryLogConfig()
