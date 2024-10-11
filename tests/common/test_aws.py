from unittest.mock import MagicMock, call

import pytest
from botocore.exceptions import ParamValidationError, ProfileNotFound
from freezegun import freeze_time

from metaphor.common.aws import AwsCredentials, iterate_logs_from_cloud_watch


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

    if not session.get_credentials():
        with pytest.raises(ProfileNotFound):
            session = conf.get_session()
    else:
        with pytest.raises(ParamValidationError):
            session = conf.get_session()


@freeze_time("2000-01-01")
def test_collect_query_logs_from_cloud_watch():
    mocked_client = MagicMock()
    mocked_client.filter_log_events.side_effect = [
        {"nextToken": "token", "events": [{"message": ""}]},
        {
            "events": [
                {
                    "message": "2024-08-29 09:25:50 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: statement: SELECT x, y from schema.table;",
                },
                {
                    "message": "2024-08-29 09:25:51 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: duration: 55.66 ms",
                },
            ]
        },
    ]

    iterator = iterate_logs_from_cloud_watch(
        mocked_client, lookback_days=1, logs_group="123"
    )
    assert next(iterator) == ""
    assert (
        next(iterator)
        == "2024-08-29 09:25:50 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: statement: SELECT x, y from schema.table;"
    )
    assert (
        next(iterator)
        == "2024-08-29 09:25:51 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: duration: 55.66 ms"
    )

    with pytest.raises(StopIteration):
        next(iterator)
    mocked_client.filter_log_events.assert_has_calls(
        [
            call(
                logGroupName="123",
                startTime=946598400000,
                endTime=946684800000,
            ),
            call(
                logGroupName="123",
                startTime=946598400000,
                endTime=946684800000,
                nextToken="token",
            ),
        ]
    )
