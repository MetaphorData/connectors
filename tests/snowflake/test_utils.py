from metaphor.snowflake.filter import SnowflakeFilter
from metaphor.snowflake.utils import include_table


def test_include_table_empty_filter():

    filter = SnowflakeFilter(
        includes=None,
        excludes=None,
    )

    assert include_table("db1", "boo", "bar", filter)
    assert include_table("db2", "boo", "bar", filter)


def test_include_table_includes_only():

    filter = SnowflakeFilter(
        includes={
            "db1": None,
            "db2": {"schema1": None, "schema2": set(["table1", "table2"])},
        },
        excludes=None,
    )

    assert include_table("db1", "foo", "bar", filter)

    assert include_table("db2", "schema1", "foo", filter)
    assert include_table("db2", "schema2", "table1", filter)
    assert include_table("db2", "schema2", "table2", filter)
    assert not include_table("db2", "schema2", "foo", filter)
    assert not include_table("db2", "schema3", "foo", filter)

    assert not include_table("db3", "foo", "bar", filter)


def test_include_table_excludes_only():

    filter = SnowflakeFilter(
        includes=None,
        excludes={
            "db1": None,
            "db2": {"schema1": None, "schema2": set(["table1", "table2"])},
        },
    )

    assert not include_table("db1", "foo", "bar", filter)

    assert not include_table("db2", "schema1", "foo", filter)
    assert not include_table("db2", "schema2", "table1", filter)
    assert not include_table("db2", "schema2", "table2", filter)

    assert include_table("db3", "foo", "bar", filter)


def test_include_table_excludes_overrides_include():

    filter = SnowflakeFilter(
        includes={
            "db1": None,
        },
        excludes={"db1": {"schema1": None, "schema2": set(["table1", "table2"])}},
    )

    assert include_table("db1", "foo", "bar", filter)
    assert not include_table("db1", "schema1", "foo", filter)
    assert not include_table("db1", "schema2", "table1", filter)
    assert not include_table("db1", "schema2", "table2", filter)
    assert include_table("db1", "schema2", "foo", filter)
