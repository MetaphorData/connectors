import os
import tempfile
from unittest.mock import ANY, MagicMock, patch

from metaphor.common.storage import LocalStorage, S3Storage, S3StorageConfig


def test_parse_s3_uri():
    s3_storage = S3Storage()

    bucket, key = s3_storage.parse_s3_uri("s3://buc/foo/bar")
    assert bucket == "buc"
    assert key == "foo/bar"

    bucket, key = s3_storage.parse_s3_uri("s3://buc")
    assert bucket == "buc"
    assert key == ""

    bucket, key = s3_storage.parse_s3_uri("s3://buc/")
    assert bucket == "buc"
    assert key == ""


def test_local_storage(test_root_dir):
    storage = LocalStorage()

    # non-exist directory
    assert storage.list_files("foo/bar/abc123", ".json") == []

    # mkstemp may create more than 1 temp files
    _, temp_file = tempfile.mkstemp(suffix=".tmp")
    assert len(storage.list_files(os.path.dirname(temp_file), ".tmp")) > 0


@patch("metaphor.common.storage.boto3.Session")
@patch("metaphor.common.storage.assume_role")
def test_s3_storage_configs(mock_assume_role, mock_session_class, test_root_dir):
    S3Storage(
        assume_role_arn="arn",
        config=S3StorageConfig(
            aws_access_key_id="id", aws_secret_access_key="secret", region_name="region"
        ),
    )

    mock_session = MagicMock()
    mock_session_class.side_effect = mock_session

    mock_session_class.assert_called_with(
        aws_access_key_id="id", aws_secret_access_key="secret", region_name="region"
    )

    mock_assume_role.assert_called_with(ANY, "arn")
