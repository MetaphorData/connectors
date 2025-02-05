from dataclasses import field
from typing import List, Set

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.sql.process_query import ProcessQueryConfig
from metaphor.common.tag_matcher import TagMatcher
from metaphor.snowflake.auth import SnowflakeAuthConfig
from metaphor.snowflake.utils import DEFAULT_THREAD_POOL_SIZE

# number of query logs to fetch from Snowflake in one batch
DEFAULT_QUERY_LOG_FETCH_SIZE = 100000

# By default ignore queries longer than 100K characters
# Reference: https://docs.snowflake.com/en/sql-reference/account-usage/query_history
DEFAULT_MAX_QUERY_SIZE = 100_000


@dataclass(config=ConnectorConfig)
class SnowflakeQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # The number of query logs to fetch from Snowflake in one batch
    fetch_size: int = DEFAULT_QUERY_LOG_FETCH_SIZE

    # Queries larger than this size will not be processed
    max_query_size: int = DEFAULT_MAX_QUERY_SIZE

    # Config to control query processing
    process_query: ProcessQueryConfig = field(
        default_factory=lambda: ProcessQueryConfig()
    )


@dataclass(config=ConnectorConfig)
class SnowflakeStreamsConfig:
    # Enable fetching Snowflake Streams metadata
    enabled: bool = True

    # Count rows in stream - disabled by default as it can be expensive to count
    count_rows: bool = False


@dataclass(config=ConnectorConfig)
class SnowflakeBaseConfig(SnowflakeAuthConfig):
    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # Max number of concurrent queries to database
    max_concurrency: int = DEFAULT_THREAD_POOL_SIZE

    # The fully qualified schema that contains all the account_usage views
    account_usage_schema: str = "SNOWFLAKE.ACCOUNT_USAGE"

    # Whether to collect platform tags.
    collect_tags: bool = True


@dataclass(config=ConnectorConfig)
class SnowflakeLineageConfig:
    # Whether to enable finding view lineage from object dependencies, default True
    enable_view_lineage: bool = True


@dataclass(config=ConnectorConfig)
class SnowflakeConfig(SnowflakeBaseConfig):
    # How tags should be assigned to datasets
    tag_matchers: List[TagMatcher] = field(default_factory=lambda: [])

    # configs for fetching query logs
    query_log: SnowflakeQueryLogConfig = field(
        default_factory=lambda: SnowflakeQueryLogConfig()
    )

    # configs for fetching Snowflake streams
    streams: SnowflakeStreamsConfig = field(
        default_factory=lambda: SnowflakeStreamsConfig()
    )

    # configs for lineage information
    lineage: SnowflakeLineageConfig = field(
        default_factory=lambda: SnowflakeLineageConfig()
    )
