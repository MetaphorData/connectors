from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.mssql.config import MssqlConfig


@dataclass(config=ConnectorConfig)
class SynapseQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 0


@dataclass(config=ConnectorConfig)
class SynapseConfig(MssqlConfig):
    query_log: Optional[SynapseQueryLogConfig] = field(
        default_factory=lambda: SynapseQueryLogConfig()
    )
