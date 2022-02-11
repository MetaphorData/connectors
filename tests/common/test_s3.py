from metaphor.common.s3 import parse_s3_uri


def test_parse_s3_uri():
    bucket, key = parse_s3_uri("s3://buc/foo/bar")
    assert bucket == "buc"
    assert key == "foo/bar"

    bucket, key = parse_s3_uri("s3://buc")
    assert bucket == "buc"
    assert key == ""

    bucket, key = parse_s3_uri("s3://buc/")
    assert bucket == "buc"
    assert key == ""
