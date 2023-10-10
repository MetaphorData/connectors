from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class MonteCarloRunConfig(BaseConfig):
    api_key_id: str
    api_key_secret: str

    # Snowflake data source account
    snowflake_account: Optional[str] = None
