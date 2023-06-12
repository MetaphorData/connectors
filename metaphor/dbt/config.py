from dataclasses import field as dataclass_field
from typing import List, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class MetaOwnership:
    # Key to match in the "meta" field
    meta_key: str

    # Type of ownership to assign
    ownership_type: str

    # Domain for user names
    email_domain: Optional[str] = None


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
    catalog: Optional[str] = None

    # the database service account this DBT project is connected to
    account: Optional[str] = None

    # the dbt docs base URL
    docs_base_url: Optional[str] = None

    # the source code URL for the project directory
    project_source_url: Optional[str] = None

    # map meta field to ownerships
    meta_ownerships: List[MetaOwnership] = dataclass_field(default_factory=lambda: [])

    # map meta field to tags
    meta_tags: List[MetaTag] = dataclass_field(default_factory=lambda: [])
