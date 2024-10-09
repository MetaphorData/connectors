from dataclasses import field
from typing import Optional, Set

from pydantic.dataclasses import dataclass

from metaphor.common.aws import AwsCredentials
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.sql.process_query.config import ProcessQueryConfig
from metaphor.database.config import GenericDatabaseRunConfig


@dataclass(config=ConnectorConfig)
class MySQLQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # Config to control query processing
    process_query: ProcessQueryConfig = field(
        default_factory=lambda: ProcessQueryConfig(ignore_command_statement=True)
    )

    aws: Optional[AwsCredentials] = None
    logs_group: Optional[str] = None


@dataclass(config=ConnectorConfig)
class MySQLRunConfig(GenericDatabaseRunConfig):
    port: int = 3306

    # configs for fetching query logs
    query_log: MySQLQueryLogConfig = field(
        default_factory=lambda: MySQLQueryLogConfig()
    )
