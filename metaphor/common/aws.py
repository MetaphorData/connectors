from datetime import datetime, timezone
from typing import Iterator, Optional

from aws_assume_role_lib import assume_role
from boto3 import Session, client
from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.logger import get_logger
from metaphor.common.utils import start_of_day

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


def iterate_logs_from_cloud_watch(
    client: client,
    lookback_days: int,
    logs_group: str,
    filter_pattern: Optional[str] = None,
) -> Iterator[str]:

    logger.info(f"Collecting query log from cloud watch for {lookback_days} days")

    # Start time in milliseconds since epoch
    start_timestamp_ms = int(start_of_day(lookback_days).timestamp() * 1000)
    # End time in milliseconds since epoch
    end_timestamp_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    next_token = None
    count = 0

    while True:
        params = {
            "logGroupName": logs_group,
            "startTime": start_timestamp_ms,
            "endTime": end_timestamp_ms,
        }
        if next_token:
            params["nextToken"] = next_token
        if filter_pattern:
            params["filterPattern"] = filter_pattern
        response = client.filter_log_events(**params)

        next_token = response["nextToken"] if "nextToken" in response else None

        for event in response["events"]:
            message = event["message"]
            count += 1

            if count % 1000 == 0:
                logger.info(f"Processed {count} logs")

            yield message

        if next_token is None:
            break
