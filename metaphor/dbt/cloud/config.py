from dataclasses import field as dataclass_field
from typing import List, Optional, Set

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.dbt.config import MetaOwnership, MetaTag


@dataclass(config=ConnectorConfig)
class DbtCloudConfig(BaseConfig):
    # dbt cloud account ID
    account_id: int

    # Service token for dbt cloud
    service_token: str

    # dbt cloud job IDs, deprecated and will not be used
    job_ids: Set[int] = dataclass_field(default_factory=set)

    # dbt cloud project IDs
    project_ids: Set[int] = dataclass_field(default_factory=set)

    # dbt cloud environment IDs to include. If specified, only jobs run in the provided environments will be crawled.
    environment_ids: Set[int] = dataclass_field(default_factory=set)

    # map meta field to ownerships
    meta_ownerships: List[MetaOwnership] = dataclass_field(default_factory=list)

    # Deprecated. Use meta_key_tags instead
    meta_tags: List[MetaTag] = dataclass_field(default_factory=list)

    # Maps meta field to additional dbt tags
    meta_key_tags: Optional[str] = None

    # Base URL for dbt instance
    base_url: str = "https://cloud.getdbt.com"

    # Discovery API endpoint
    discovery_api_url: str = "https://metadata.cloud.getdbt.com/graphql"
