from pydantic.dataclasses import dataclass

from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class QuickSightRunConfig(BaseConfig):
    aws: AwsCredentials

    aws_account_id: str
