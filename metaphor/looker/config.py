from typing import Dict, Optional

from pydantic import root_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.git import GitRepoConfig
from metaphor.common.utils import must_set_exactly_one
from metaphor.models.metadata_change_event import DataPlatform


@dataclass(config=ConnectorConfig)
class LookerConnectionConfig:
    # Database for the connection
    database: str

    # "Account" portion of the resulting Dataset Logical ID.
    # Use Snowflake account for Snowflake connection.
    account: Optional[str] = None

    # Default schema for the connection
    default_schema: Optional[str] = None

    # "Platform" portion of the resulting Dataset Logical ID.
    platform: DataPlatform = DataPlatform.SNOWFLAKE


@dataclass(config=ConnectorConfig)
class LookerRunConfig(BaseConfig):
    base_url: str
    client_id: str
    client_secret: str

    # A map of Looker connection names to connection config
    connections: Dict[str, LookerConnectionConfig]

    # File path to LookML project directory
    lookml_dir: Optional[str] = None

    # LookML git repository configuration
    lookml_git_repo: Optional[GitRepoConfig] = None

    # Source code URL for the project directory
    project_source_url: Optional[str] = None

    verify_ssl: bool = True
    timeout: int = 120

    @root_validator
    def have_local_or_git_dir_for_lookml(cls, values):
        must_set_exactly_one(values, ["lookml_dir", "lookml_git_repo"])
        return values
