import pytest

from metaphor.models.metadata_change_event import DataPlatform
from tests.common.sql.table_level_lineage.utils import assert_table_lineage_equal

"""
This test class will contain all the tests for testing 'Select Queries' where the platform is not ANSI.
"""


@pytest.mark.parametrize(
    "platform",
    [
        DataPlatform.BIGQUERY,
        DataPlatform.UNITY_CATALOG,
        DataPlatform.HIVE,
        DataPlatform.MYSQL,
    ],  # TODO: athena, sparksql
)
def test_select_with_table_name_in_backtick(platform: DataPlatform):
    assert_table_lineage_equal(
        "SELECT * FROM `tab1`", {"tab1"}, None, platform=platform
    )


@pytest.mark.parametrize(
    "platform",
    [
        DataPlatform.BIGQUERY,
        DataPlatform.UNITY_CATALOG,
        DataPlatform.HIVE,
        DataPlatform.MYSQL,
    ],  # TODO: athena, sparksql
)
def test_select_with_schema_in_backtick(platform: DataPlatform):
    assert_table_lineage_equal(
        "SELECT col1 FROM `schema1`.`tab1`",
        {"schema1.tab1"},
        None,
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]  # TODO: sparksql
)
def test_select_left_semi_join(platform: DataPlatform):
    assert_table_lineage_equal(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2",
        {"tab1", "tab2"},
        None,
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]  # TODO: sparksql
)
def test_select_left_semi_join_with_on(platform: DataPlatform):
    assert_table_lineage_equal(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2 ON (tab1.col1 = tab2.col2)",
        {"tab1", "tab2"},
        None,
        platform=platform,
    )


@pytest.mark.parametrize(
    "platform", [DataPlatform.POSTGRESQL, DataPlatform.REDSHIFT, DataPlatform.MSSQL]
)
def test_select_into(platform: DataPlatform):
    """
    postgres: https://www.postgresql.org/docs/current/sql-selectinto.html
    redshift: https://docs.aws.amazon.com/redshift/latest/dg/r_SELECT_INTO.html
    tsql: https://learn.microsoft.com/en-us/sql/t-sql/queries/select-into-clause-transact-sql?view=sql-server-ver16
    """
    sql = "SELECT * INTO films_recent FROM films WHERE date_prod >= '2002-01-01'"
    assert_table_lineage_equal(sql, {"films"}, {"films_recent"}, platform=platform)


@pytest.mark.parametrize("platform", [DataPlatform.POSTGRESQL, DataPlatform.MSSQL])
def test_select_into_with_union(platform: DataPlatform):
    sql = "SELECT * INTO films_all FROM films UNION ALL SELECT * FROM films_backup"
    assert_table_lineage_equal(
        sql, {"films", "films_backup"}, {"films_all"}, platform=platform
    )


@pytest.mark.skip("Metaphor does not support athena")
@pytest.mark.parametrize("platform", ["athena"])
def test_select_from_unnest_with_ordinality(platform: DataPlatform):
    """
    https://prestodb.io/docs/current/sql/select.html#unnest
    """
    sql = """
    SELECT numbers, n, a
    FROM (
      VALUES
        (ARRAY[2, 5]),
        (ARRAY[7, 8, 9])
    ) AS x (numbers)
    CROSS JOIN UNNEST(numbers) WITH ORDINALITY AS t (n, a);
    """
    assert_table_lineage_equal(sql, None, None, platform=platform)
