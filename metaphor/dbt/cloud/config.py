from dataclasses import field as dataclass_field
from typing import List

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.dbt.config import MetaOwnership, MetaTag


@dataclass
class DbtCloudConfig(BaseConfig):
    # dbt cloud account ID
    account_id: int

    # dbt cloud job ID
    job_id: int

    # Service token for dbt cloud
    service_token: str

    # map meta field to ownerships
    meta_ownerships: List[MetaOwnership] = dataclass_field(default_factory=lambda: [])

    # map meta field to tags
    meta_tags: List[MetaTag] = dataclass_field(default_factory=lambda: [])
