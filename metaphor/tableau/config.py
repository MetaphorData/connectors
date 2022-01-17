from dataclasses import dataclass
from typing import Optional

from serde import deserialize

from metaphor.common.extractor import RunConfig


@deserialize
@dataclass
class TableauTokenAuthConfig:
    """Config for Personal Access Token authentication"""

    token_name: str
    token_value: str


@deserialize
@dataclass
class TableauPasswordAuthConfig:
    """Config for Username Password authentication"""

    username: str
    password: str


@deserialize
@dataclass
class TableauRunConfig(RunConfig):
    server_url: str
    site_name: str
    access_token: Optional[TableauTokenAuthConfig]
    user_password: Optional[TableauPasswordAuthConfig]

    # snowflake data source account
    snowflake_account: Optional[str]
