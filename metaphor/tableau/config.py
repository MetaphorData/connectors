import dataclasses
from typing import Dict, List, Optional, Set

from pydantic import model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.utils import must_set_exactly_one

PERSONAL_SPACE_PROJECT_NAME = "Personal Space"


@dataclass(config=ConnectorConfig)
class TableauTokenAuthConfig:
    """Config for Personal Access Token authentication"""

    token_name: str
    token_value: str


@dataclass(config=ConnectorConfig)
class TableauPasswordAuthConfig:
    """Config for Username Password authentication"""

    username: str
    password: str


@dataclass(config=ConnectorConfig)
class TableauRunConfig(BaseConfig):
    server_url: str
    site_name: str
    access_token: Optional[TableauTokenAuthConfig] = None
    user_password: Optional[TableauPasswordAuthConfig] = None

    alternative_base_url: Optional[str] = None

    # Snowflake data source account
    snowflake_account: Optional[str] = None

    # BigQuery data source project name to project ID map
    bigquery_project_name_to_id_map: Dict[str, str] = dataclasses.field(
        default_factory=dict
    )

    exclude_extra_projects: List[str] = dataclasses.field(default_factory=list)
    include_personal_space: bool = False

    # whether to disable Chart preview image
    disable_preview_image: bool = False

    @model_validator(mode="after")
    def have_access_token_or_user_password(self):
        must_set_exactly_one(self.__dict__, ["access_token", "user_password"])
        return self

    @property
    def excluded_projects(self) -> Set[str]:
        return set(
            self.exclude_extra_projects
            + ([] if self.include_personal_space else [PERSONAL_SPACE_PROJECT_NAME])
        )
