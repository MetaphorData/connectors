from dataclasses import field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.database.config import GenericDatabaseRunConfig


@dataclass(config=ConnectorConfig)
class OracleQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=set)


@dataclass(config=ConnectorConfig)
class OracleRunConfig(GenericDatabaseRunConfig):
    port: int = 1521

    # configs for fetching query logs
    query_logs: OracleQueryLogConfig = field(
        default_factory=lambda: OracleQueryLogConfig()
    )
