from dataclasses import field
from typing import Dict, Optional, Set

from pydantic import field_validator
from pydantic.dataclasses import dataclass

from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.sql.process_query.config import ProcessQueryConfig


@dataclass(config=ConnectorConfig)
class QueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=set)

    # Config to control query processing
    process_query: ProcessQueryConfig = field(
        default_factory=lambda: ProcessQueryConfig(
            ignore_command_statement=True
        )  # Ignore COMMAND statements by default
    )

    # Config to link user name to email so that Metaphor can display each query's issuer.
    username_to_email: Dict[str, str] = field(default_factory=dict)

    @field_validator("username_to_email")
    def _normalize_emails(cls, username_to_email: Dict[str, str]):
        return {
            k: v.lower()
            for k, v
            in username_to_email.items()
        }


@dataclass(config=ConnectorConfig)
class BasePostgreSQLRunConfig(BaseConfig):
    host: str
    database: str
    user: str
    password: str

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # configs for fetching query logs
    query_log: QueryLogConfig = field(default_factory=lambda: QueryLogConfig())

    port: int = 5432


@dataclass(config=ConnectorConfig)
class PostgreSQLQueryLogConfig(QueryLogConfig):
    aws: Optional[AwsCredentials] = None
    logs_group: Optional[str] = None


@dataclass(config=ConnectorConfig)
class PostgreSQLRunConfig(BasePostgreSQLRunConfig):
    # configs for fetching query logs
    query_log: PostgreSQLQueryLogConfig = field(
        default_factory=lambda: PostgreSQLQueryLogConfig()
    )
