import pytest

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


@pytest.mark.skip
def test_exclude_table_wildcard() -> None:
    path_spec = PathSpec(
        uri="s3://bucket/v1/services/data/{table}/v1/*/{partition[0]}/{partition[1]}/{partition[2]}/*.*",
        file_types={
            "parquet",
        },
        excludes=["s3://bucket/v1/services/data/skipped_*"],
    )
    assert not path_spec.allow_path("s3://bucket/v1/services/data/skipped_0/")
