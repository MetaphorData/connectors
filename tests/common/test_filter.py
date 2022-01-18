from metaphor.common.filter import DatasetFilter, include_table


def test_filter_normalization():
    config = DatasetFilter(
        includes={"DB": {"schema1": None, "SCHEMA2": set(["TABLE1", "table2"])}},
        excludes={"db": {"Schema1": None}},
    )

    assert config.normalize() == DatasetFilter(
        includes={"db": {"schema1": None, "schema2": set(["table1", "table2"])}},
        excludes={"db": {"schema1": None}},
    )


def test_include_table_empty_filter():

    filter = DatasetFilter(
        includes=None,
        excludes=None,
    )

    assert include_table("db1", "boo", "bar", filter)
    assert include_table("db2", "boo", "bar", filter)


def test_include_table_includes_only():

    filter = DatasetFilter(
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

    filter = DatasetFilter(
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

    filter = DatasetFilter(
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
