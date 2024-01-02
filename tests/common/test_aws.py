import pytest
from botocore.exceptions import NoCredentialsError, ProfileNotFound

from metaphor.common.aws import AwsCredentials


def test_aws_session() -> None:
    conf = AwsCredentials(region_name="us-west-2")
    session = conf.get_session()
    assert session.profile_name == "default"

    conf = AwsCredentials(region_name="us-west-2", profile_name="foo")
    with pytest.raises(ProfileNotFound):
        session = conf.get_session()

    conf = AwsCredentials(
        region_name="us-west-2", profile_name="default", assume_role_arn="role:arn"
    )
    with pytest.raises(NoCredentialsError):
        session = conf.get_session()
