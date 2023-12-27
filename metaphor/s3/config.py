from typing import Union
from pydantic.dataclasses import dataclass
from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class S3RunConfig(BaseConfig):
    aws: AwsCredentials
    endpoint_url: str
    verify_ssl: Union[bool, str] = False

    def s3_client(self):
        return self.aws.get_session().client(
            service_name="s3",
            endpoint_url=self.endpoint_url,
            verify=self.verify_ssl,
        )