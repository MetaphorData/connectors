from dataclasses import field
from typing import List, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class MonteCarloRunConfig(BaseConfig):
    api_key_id: str
    api_key_secret: str

    # Snowflake data source account
    snowflake_account: Optional[str] = None

    # Treat unhandled anomalies as errors
    treat_unhandled_anomalies_as_errors: bool = False

    # Number of days to look back for anomalies
    anomalies_lookback_days = 30

    # Errors to be ignored, e.g. timeouts
    ignored_errors: List[str] = field(
        default_factory=lambda: [
            "Monitor timed out",
        ]
    )
