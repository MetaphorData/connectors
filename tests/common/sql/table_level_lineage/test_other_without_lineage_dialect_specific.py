import pytest

from metaphor.models.metadata_change_event import DataPlatform
from tests.common.sql.table_level_lineage.utils import assert_table_lineage_equal


@pytest.mark.parametrize("platform", [DataPlatform.MYSQL])  # TODO: exasol, teradata
def test_rename_table(platform: DataPlatform):
    """
    https://docs.exasol.com/db/latest/sql/rename.htm
    https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
    https://docs.teradata.com/r/Teradata-Database-SQL-Data-Definition-Language-Syntax-and-Examples/December-2015/Table-Statements/RENAME-TABLE
    """
    assert_table_lineage_equal(
        "rename table tab1 to tab2", None, None, platform=platform
    )


@pytest.mark.parametrize("platform", [DataPlatform.MYSQL])
def test_rename_tables(platform: DataPlatform):
    assert_table_lineage_equal(
        "rename table tab1 to tab2, tab3 to tab4",
        None,
        None,
        platform=platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_refresh_table(platform: DataPlatform):
    assert_table_lineage_equal("refresh table tab1", None, None, platform=platform)


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_cache_table(platform: DataPlatform):
    assert_table_lineage_equal(
        "cache table tab1 select * from tab2", None, None, platform=platform
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_uncache_table(platform: DataPlatform):
    assert_table_lineage_equal("uncache table tab1", None, None, platform=platform)


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_uncache_table_if_exists(platform: DataPlatform):
    assert_table_lineage_equal(
        "uncache table if exists tab1", None, None, platform=platform
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_show_create_table(platform: DataPlatform):
    assert_table_lineage_equal("show create table tab1", None, None, platform=platform)


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_add_jar(platform: DataPlatform):
    assert_table_lineage_equal(
        "ADD JAR /tmp/SimpleUdf.jar", None, None, platform=platform
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_create_function(platform: DataPlatform):
    assert_table_lineage_equal(
        """CREATE FUNCTION simple_udf AS 'SimpleUdf'
  USING JAR '/tmp/SimpleUdf.jar'""",
        None,
        None,
        platform=platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_create_or_replace_function(platform: DataPlatform):
    assert_table_lineage_equal(
        """CREATE OR REPLACE FUNCTION simple_udf AS 'SimpleUdf'
      USING JAR '/tmp/SimpleUdf.jar'""",
        None,
        None,
        platform=platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_create_temporary_function(platform: DataPlatform):
    assert_table_lineage_equal(
        """CREATE TEMPORARY FUNCTION simple_temp_udf AS 'SimpleUdf'
  USING JAR '/tmp/SimpleUdf.jar'""",
        None,
        None,
        platform=platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_create_or_replace_temporary_function(platform: DataPlatform):
    assert_table_lineage_equal(
        """CREATE OR REPLACE TEMPORARY FUNCTION simple_temp_udf AS 'SimpleUdf'
  USING JAR '/tmp/SimpleUdf.jar'""",
        None,
        None,
        platform=platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_show_functions(platform: DataPlatform):
    assert_table_lineage_equal("SHOW FUNCTIONS trim", None, None, platform=platform)


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_describe_function(platform: DataPlatform):
    assert_table_lineage_equal("DESC FUNCTION abs", None, None, platform=platform)


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_drop_function(platform: DataPlatform):
    assert_table_lineage_equal("DROP FUNCTION test_avg", None, None, platform=platform)


@pytest.mark.parametrize(
    "platform", [DataPlatform.UNITY_CATALOG, DataPlatform.HIVE]
)  # TODO: sparksql
def test_set_command(platform: DataPlatform):
    assert_table_lineage_equal(
        "SET spark.sql.variable.substitute=false",
        None,
        None,
        platform=platform,
    )


@pytest.mark.parametrize("platform", [DataPlatform.UNITY_CATALOG])  # TODO: sparksql
def test_set_command_without_property_value(platform: DataPlatform):
    assert_table_lineage_equal(
        "SET spark.sql.variable.substitute", None, None, platform=platform
    )


@pytest.mark.parametrize("platform", [DataPlatform.POSTGRESQL, DataPlatform.REDSHIFT])
def test_analyze_table(platform: DataPlatform):
    assert_table_lineage_equal("analyze tab", None, None, platform=platform)


@pytest.mark.parametrize("platform", [DataPlatform.POSTGRESQL, DataPlatform.REDSHIFT])
def test_analyze_table_column(platform: DataPlatform):
    assert_table_lineage_equal(
        "analyze tab (col1, col2)", None, None, platform=platform
    )
