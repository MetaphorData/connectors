from typing import Optional

from aws_assume_role_lib import assume_role
from boto3 import Session
from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.logger import get_logger

logger = get_logger()


@dataclass(config=ConnectorConfig)
class AwsCredentials:
    """AWS Credentials"""

    # The aws region containing the service instance
    region_name: str

    # The access key of an aws credentials
    access_key_id: Optional[str] = None

    # The secret access key of an aws credentials
    secret_access_key: Optional[str] = None

    # The aws session token.
    session_token: Optional[str] = None

    # Provide a role arn if assume role is needed
    assume_role_arn: Optional[str] = None

    # Name of an aws profile
    profile_name: Optional[str] = None

    def get_session(self):
        if self.access_key_id and self.secret_access_key:
            session = Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                aws_session_token=self.session_token,
                region_name=self.region_name,
            )
        elif self.profile_name:
            session = Session(
                region_name=self.region_name, profile_name=self.profile_name
            )
        else:
            # Use boto3's credential autodetection.
            session = Session(region_name=self.region_name)

        if self.assume_role_arn is not None:
            session = assume_role(session, self.assume_role_arn)
            logger.info(f"Assumed role: {session.client('sts').get_caller_identity()}")

        return session
