from dataclasses import field as dataclass_field
from typing import List, Literal, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig

MetaOwnershipAssignmentTarget = Literal["dbt_model", "materialized_table", "both"]


@dataclass(config=ConnectorConfig)
class MetaOwnership:
    # Key to match in the "meta" field
    meta_key: str

    # Type of ownership to assign
    ownership_type: str

    # Domain for user names
    email_domain: Optional[str] = None

    # The target to assign this ownership to. Can be either the dbt model, the materialized table, or both.
    # Defaults to both.
    assignment_target: MetaOwnershipAssignmentTarget = "both"


@dataclass(config=ConnectorConfig)
class MetaTag:
    # Key to match in the "meta" field
    meta_key: str

    # Type of the tag to assign
    tag_type: str

    # Regex to match the value
    meta_value_matcher: str = "True"


@dataclass(config=ConnectorConfig)
class DbtRunConfig(BaseConfig):
    manifest: str

    run_results: Optional[str] = None

    # the database service account this DBT project is connected to
    account: Optional[str] = None

    # the dbt docs base URL
    docs_base_url: Optional[str] = None

    # the source code URL for the project directory
    project_source_url: Optional[str] = None

    # map meta field to ownerships
    meta_ownerships: List[MetaOwnership] = dataclass_field(default_factory=lambda: [])

    # Deprecated. Use meta_key_tags instead
    meta_tags: List[MetaTag] = dataclass_field(default_factory=lambda: [])

    # Maps meta field to additional dbt tags
    meta_key_tags: Optional[str] = None
