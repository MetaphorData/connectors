from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.models.metadata_change_event import DataPlatform


@dataclass(config=ConnectorConfig)
class MonteCarloRunConfig(BaseConfig):
    api_key_id: str
    api_key_secret: str

    # (Do not use) data platform of the monitored assets
    data_platform: Optional[DataPlatform] = None

    # Snowflake data source account
    snowflake_account: Optional[str] = None
