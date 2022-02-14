from metaphor.common.storage import S3Storage


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
