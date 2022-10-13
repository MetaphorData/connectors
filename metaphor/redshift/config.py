from dataclasses import field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.postgresql.config import PostgreSQLRunConfig


@dataclass
class RedshiftQueryLogConfig:

    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())


@dataclass
class RedshiftRunConfig(PostgreSQLRunConfig):
    port: int = 5439

    # configs for fetching query logs
    query_log: RedshiftQueryLogConfig = RedshiftQueryLogConfig()
