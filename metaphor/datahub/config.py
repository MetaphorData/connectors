from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.models.metadata_change_event import DataPlatform


@dataclass(config=ConnectorConfig)
class DatahubConfig(BaseConfig):
    host: str
    port: int
    token: str

    description_author_email: Optional[str] = None
    snowflake_account: Optional[str] = None
    mssql_account: Optional[str] = None
    synapse_account: Optional[str] = None

    def get_account(self, data_platform: DataPlatform) -> Optional[str]:
        if data_platform is DataPlatform.SNOWFLAKE:
            return self.snowflake_account
        if data_platform is DataPlatform.MSSQL:
            return self.mssql_account
        if data_platform is DataPlatform.SYNAPSE:
            return self.synapse_account
        return None
