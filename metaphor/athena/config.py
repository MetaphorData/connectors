from dataclasses import field
from typing import List

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

    # (Optional) WorkGroups to collect query history, default to []. If not specify, collect from the primary workgroup
    work_groups: List[str] = field(default_factory=list)

    # Config to control query processing
    process_query: ProcessQueryConfig = field(
        default_factory=lambda: ProcessQueryConfig()
    )


@dataclass(config=ConnectorConfig)
class AthenaRunConfig(BaseConfig):
    aws: AwsCredentials

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # configs for fetching query logs
    query_log: QueryLogConfig = field(default_factory=lambda: QueryLogConfig())
