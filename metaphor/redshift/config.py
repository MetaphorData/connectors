from dataclasses import field
from typing import List, Set

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.tag_matcher import TagMatcher
from metaphor.postgresql.config import PostgreSQLRunConfig


@dataclass(config=ConnectorConfig)
class RedshiftQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())


@dataclass(config=ConnectorConfig)
class RedshiftRunConfig(PostgreSQLRunConfig):
    port: int = 5439

    # How tags should be assigned to datasets
    tag_matchers: List[TagMatcher] = field(default_factory=lambda: [])

    # configs for fetching query logs
    query_log: RedshiftQueryLogConfig = RedshiftQueryLogConfig()
