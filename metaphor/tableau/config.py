import dataclasses
from typing import Dict, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class TableauTokenAuthConfig:
    """Config for Personal Access Token authentication"""

    token_name: str
    token_value: str


@dataclass
class TableauPasswordAuthConfig:
    """Config for Username Password authentication"""

    username: str
    password: str


@dataclass
class TableauRunConfig(BaseConfig):
    server_url: str
    site_name: str
    access_token: Optional[TableauTokenAuthConfig] = None
    user_password: Optional[TableauPasswordAuthConfig] = None

    # Snowflake data source account
    snowflake_account: Optional[str] = None

    # BigQuery data source project name to project ID map
    bigquery_project_name_to_id_map: Dict[str, str] = dataclasses.field(
        default_factory=dict
    )

    # whether to disable Chart preview image
    disable_preview_image: bool = False
