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

    # Errors to be ignored, e.g. timeouts
    ignored_errors: List[str] = field(
        default_factory=lambda: [
            "Monitor timed out",
        ]
    )
