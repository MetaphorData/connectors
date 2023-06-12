from dataclasses import field as dataclass_field
from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter


@dataclass(config=ConnectorConfig)
class AwsCredentials:
    """AWS Credentials"""

    # The access key of an aws credentials
    access_key_id: str

    # The secret access key of an aws credentials
    secret_access_key: str

    # The aws region containing the Glue
    region_name: str

    # Provide a role arn if assume role is needed
    assume_role_arn: Optional[str] = None


@dataclass(config=ConnectorConfig)
class GlueRunConfig(BaseConfig):
    aws: AwsCredentials

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = dataclass_field(default_factory=lambda: DatasetFilter())
