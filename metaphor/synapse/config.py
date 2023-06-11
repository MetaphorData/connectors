from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import DataclassConfig
from metaphor.mssql.config import MssqlConfig


@dataclass(config=DataclassConfig)
class SynapseQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 0


@dataclass(config=DataclassConfig)
class SynapseConfig(MssqlConfig):

    query_log: Optional[SynapseQueryLogConfig] = SynapseQueryLogConfig()
