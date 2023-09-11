from dataclasses import field as dataclass_field
from typing import List, Optional

from pydantic import root_validator
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

    # (Deprecated. Use job_ids instead) dbt cloud job ID
    job_id: Optional[int] = None

    # dbt cloud job IDs
    job_ids: List[int] = dataclass_field(default_factory=lambda: [])

    # map meta field to ownerships
    meta_ownerships: List[MetaOwnership] = dataclass_field(default_factory=lambda: [])

    # map meta field to tags
    meta_tags: List[MetaTag] = dataclass_field(default_factory=lambda: [])

    # Base URL for dbt instance
    base_url: str = "https://cloud.getdbt.com"

    @root_validator
    def set_job_ids_or_job_id(cls, values):
        if values.get("job_id") is None and not values.get("job_ids"):
            raise ValueError("Must set job_ids")

        return values
