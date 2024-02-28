from dataclasses import field
from functools import cached_property
from typing import List, Optional, Union

from pydantic.dataclasses import dataclass

from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.logger import get_logger
from metaphor.s3.path_spec import PathSpec

try:
    from mypy_boto3_s3 import S3Client, S3ServiceResource
except ImportError:
    # Ignore this since mypy plugins are dev dependencies
    pass


logger = get_logger()


@dataclass(config=ConnectorConfig)
class S3RunConfig(BaseConfig):
    aws: AwsCredentials
    endpoint_url: Optional[str] = None
    path_specs: List[PathSpec] = field(default_factory=list)
    verify_ssl: Union[bool, str] = False

    @cached_property
    def s3_client(self) -> "S3Client":
        return self.aws.get_session().client(
            service_name="s3",
            endpoint_url=self.endpoint_url,
            verify=self.verify_ssl,
        )  # type: ignore

    @cached_property
    def s3_resource(self) -> "S3ServiceResource":
        return self.aws.get_session().resource(
            service_name="s3",
            endpoint_url=self.endpoint_url,
            verify=self.verify_ssl,
        )  # type: ignore
