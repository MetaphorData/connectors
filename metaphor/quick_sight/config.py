from dataclasses import field
from typing import List

from pydantic.dataclasses import dataclass

from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class QuickSightFilter:
    include_dashboard_ids: List[str] = field(default_factory=list)
    exclude_dashboard_ids: List[str] = field(default_factory=list)


@dataclass(config=ConnectorConfig)
class QuickSightRunConfig(BaseConfig):
    aws: AwsCredentials

    aws_account_id: str

    # Include or exclude specific dashboards and the related data sets
    filter: QuickSightFilter = field(default_factory=QuickSightFilter)
