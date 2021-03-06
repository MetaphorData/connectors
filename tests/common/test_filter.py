from metaphor.common.filter import DatasetFilter


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

    assert filter.include_table("db1", "boo", "bar")
    assert filter.include_table("db2", "boo", "bar")


def test_include_table_glob_patterns():

    filter = DatasetFilter(
        includes={"db?": {"schema*": set(["*"])}},
        excludes=None,
    )

    assert filter.include_table("db1", "schema1", "table1")
    assert filter.include_table("db2", "schema2", "table2")
    assert not filter.include_table("db1", "foo", "bar")
    assert not filter.include_table("db_extra", "schema1", "table1")


def test_include_table_includes_only():

    filter = DatasetFilter(
        includes={
            "db1": None,
            "db2": {"schema1": None, "schema2": set(["table1", "table2"])},
        },
        excludes=None,
    )

    assert filter.include_table("db1", "foo", "bar")

    assert filter.include_table("db2", "schema1", "foo")
    assert filter.include_table("db2", "schema2", "table1")
    assert filter.include_table("db2", "schema2", "table2")
    assert not filter.include_table("db2", "schema2", "foo")
    assert not filter.include_table("db2", "schema3", "foo")

    assert not filter.include_table("db3", "foo", "bar")


def test_include_table_excludes_only():

    filter = DatasetFilter(
        includes=None,
        excludes={
            "db1": None,
            "db2": {"schema1": None, "schema2": set(["table1", "table2"])},
        },
    )

    assert not filter.include_table("db1", "foo", "bar")

    assert not filter.include_table("db2", "schema1", "foo")
    assert not filter.include_table("db2", "schema2", "table1")
    assert not filter.include_table("db2", "schema2", "table2")

    assert filter.include_table("db3", "foo", "bar")


def test_include_table_excludes_overrides_include():

    filter = DatasetFilter(
        includes={
            "db1": None,
        },
        excludes={"db1": {"schema1": None, "schema2": set(["table1", "table2"])}},
    )

    assert filter.include_table("db1", "foo", "bar")
    assert not filter.include_table("db1", "schema1", "foo")
    assert not filter.include_table("db1", "schema2", "table1")
    assert not filter.include_table("db1", "schema2", "table2")
    assert filter.include_table("db1", "schema2", "foo")


def test_include_schema_empty_filter():

    filter = DatasetFilter(
        includes=None,
        excludes=None,
    )

    assert filter.include_schema("db1", "boo")
    assert filter.include_schema("db2", "boo")


def test_include_schema_includes_only():

    filter = DatasetFilter(
        includes={
            "db1": None,
            "db2": {"schema1": None, "schema2": set(["table1", "table2"])},
        },
        excludes=None,
    )

    assert filter.include_schema("db1", "foo")
    assert filter.include_schema("db2", "schema1")

    # partial include
    assert filter.include_schema("db2", "schema2")

    assert not filter.include_schema("db2", "schema3")
    assert not filter.include_schema("db3", "foo")


def test_include_schema_excludes_only():

    filter = DatasetFilter(
        includes=None,
        excludes={
            "db1": None,
            "db2": {"schema1": None, "schema2": set(["table1", "table2"])},
        },
    )

    assert not filter.include_schema("db1", "foo")

    assert not filter.include_schema("db2", "schema1")

    # partial exclude
    assert filter.include_schema("db2", "schema2")

    assert filter.include_schema("db3", "foo")


def test_include_schema_excludes_overrides_includes():

    filter = DatasetFilter(
        includes={
            "db1": None,
        },
        excludes={"db1": {"schema1": None, "schema2": set(["table1", "table2"])}},
    )

    assert filter.include_schema("db1", "foo")
    assert not filter.include_schema("db1", "schema1")

    # partial exclude
    assert filter.include_schema("db1", "schema2")
