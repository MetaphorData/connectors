import yarl

from metaphor.s3.path_spec import PathSpec


def test_exclude_all_files_in_directory() -> None:
    path_spec = PathSpec(
        uri="s3://bucket/v1/app/*/{table}/v4/*/{partition[0]}/{partition[1]}/{partition[2]}/*.*",
        file_types={
            "parquet",
        },
        excludes=["s3://bucket/v1/app/global/*"],
    )
    assert not path_spec.allow_path("s3://bucket/v1/app/global/foo/v4")
    assert not path_spec.allow_path("s3://bucket/v1/app/global/foo/")
    assert path_spec.allow_path("s3://bucket/v1/app/foo/bar/")


def test_exclude_table_wildcard() -> None:
    path_spec = PathSpec(
        uri="s3://bucket/v1/services/data/{table}/v1/*/{partition[0]}/{partition[1]}/{partition[2]}/*.*",
        file_types={
            "parquet",
        },
        excludes=["s3://bucket/v1/services/data/skipped_*"],
    )
    assert not path_spec.allow_path("s3://bucket/v1/services/data/skipped_0/")
    assert not path_spec.allow_path("s3://bucket/v1/services/data/skipped_1")


def test_strip_last_slash() -> None:
    assert (
        PathSpec._strip_last_slash(yarl.URL("s3://bucket/foo/bar/")).human_repr()
        == "s3://bucket/foo/bar"
    )
