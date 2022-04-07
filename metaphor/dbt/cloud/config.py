from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class DbtCloudConfig(BaseConfig):
    # dbt cloud account ID
    account_id: int

    # dbt cloud job ID
    job_id: int

    # Service token for dbt cloud
    service_token: str
