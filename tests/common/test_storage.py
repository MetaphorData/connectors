import os
import tempfile

from metaphor.common.storage import LocalStorage, S3Storage


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
