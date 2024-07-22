import pytest

from metaphor.models.metadata_change_event import DataPlatform
from tests.common.sql.table_level_lineage.utils import assert_table_lineage_equal


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_create_bucket_table_in_spark(platform: DataPlatform):
    assert_table_lineage_equal(
        """CREATE TABLE student (id INT, name STRING, age INT)
USING CSV
PARTITIONED BY (age)
CLUSTERED BY (Id) INTO 4 buckets""",
        None,
        {"student"},
        platform,
    )


@pytest.mark.skip()
@pytest.mark.parametrize("platform", [DataPlatform.UNKNOWN])  # TODO: athena
def test_create_bucket_table_in_athena(platform: DataPlatform):
    assert_table_lineage_equal(
        """CREATE TABLE bar
WITH (
  bucketed_by = ARRAY['customer_id'],
  bucket_count = 8
)
AS SELECT * FROM foo""",
        {"foo"},
        {"bar"},
        platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_create_select_without_as(platform: DataPlatform):
    assert_table_lineage_equal(
        "CREATE TABLE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"}, platform
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.POSTGRESQL, DataPlatform.REDSHIFT]
)  # TODO: duckdb, greenplum
def test_create_table_as_with_postgres_platform(platform: DataPlatform):
    """
    sqlfluff postgres family platforms parse CTAS statement as "create_table_as_statement",
    unlike "create_table_statement" in ansi platform
    """
    assert_table_lineage_equal(
        """CREATE TABLE bar AS
SELECT *
FROM foo""",
        {"foo"},
        {"bar"},
        platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.SNOWFLAKE, DataPlatform.BIGQUERY])
def test_create_clone(platform: DataPlatform):
    """
    Language manual:
        https://cloud.google.com/bigquery/docs/table-clones-create
        https://docs.snowflake.com/en/sql-reference/sql/create-clone
    """
    assert_table_lineage_equal(
        "CREATE TABLE tab2 CLONE tab1",
        {"tab1"},
        {"tab2"},
        platform=platform,
    )
