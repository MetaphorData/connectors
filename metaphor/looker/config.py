from typing import Dict, Optional

from metaphor.models.metadata_change_event import DataPlatform
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class LookerConnectionConfig:

    # Database for the connection
    database: str

    # "Account" portion of the resulting Dataset Logical ID.
    # Use Snowflake account for Snowflake connection.
    account: Optional[str] = None

    # Default schema for the connection
    default_schema: Optional[str] = None

    # "Platform" portion of the resulting Dataset Logical ID.
    platform: Optional[DataPlatform] = DataPlatform.SNOWFLAKE


@dataclass
class LookerRunConfig(BaseConfig):
    base_url: str
    client_id: str
    client_secret: str

    # File path to LookerML project directory
    lookml_dir: str

    # A map of Looker connection names to connection config
    connections: Dict[str, LookerConnectionConfig]

    # Source code URL for the project directory
    project_source_url: Optional[str] = None

    verify_ssl: bool = True
    timeout: int = 120
