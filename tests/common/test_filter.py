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


def test_include_schema_glob_patterns():
    filter = DatasetFilter(
        includes={"db": {"foo?": None, "ba*": None, "quax?": set(["a"])}},
        excludes={"db": {"bar": None, "uh*": set(["x"])}},
    )

    assert filter.include_schema("db", "foo1")
    assert filter.include_schema("db", "bar1")
    assert filter.include_schema("db", "bazz")
    assert filter.include_schema("db", "quax1")
    assert not filter.include_schema("db", "foo")
    assert not filter.include_schema("db", "bar")
    assert not filter.include_schema("db", "uhoh")

    filter = DatasetFilter(
        includes={"*": {"test*": None}},
    )

    assert filter.include_schema("foo", "test1")
    assert not filter.include_schema("foo", "bar")

    filter = DatasetFilter(
        includes={"foo*": {"test*": None}}, excludes={"foo_bar": None}
    )
    assert filter.include_schema("foo_baz", "test1")
    assert not filter.include_schema("foo_bar", "test1")


def test_merge():
    f1 = DatasetFilter()
    f2 = DatasetFilter()
    assert f1.merge(f2) == DatasetFilter()

    f1 = DatasetFilter(includes={"foo": None})
    f2 = DatasetFilter()
    assert f1.merge(f2) == DatasetFilter(includes={"foo": None})

    f1 = DatasetFilter()
    f2 = DatasetFilter(includes={"foo": None})
    assert f1.merge(f2) == DatasetFilter(includes={"foo": None})

    f1 = DatasetFilter(excludes={"foo": None})
    f2 = DatasetFilter()
    assert f1.merge(f2) == DatasetFilter(excludes={"foo": None})

    f1 = DatasetFilter()
    f2 = DatasetFilter(excludes={"foo": None})
    assert f1.merge(f2) == DatasetFilter(excludes={"foo": None})

    f1 = DatasetFilter(includes={"foo": None})
    f2 = DatasetFilter(excludes={"bar": None})
    assert f1.merge(f2) == DatasetFilter(includes={"foo": None}, excludes={"bar": None})

    f1 = DatasetFilter(includes={"foo": None})
    f2 = DatasetFilter(includes={"bar": None})
    assert f1.merge(f2) == DatasetFilter(includes={"foo": None, "bar": None})

    f1 = DatasetFilter(includes={"foo": None})
    f2 = DatasetFilter(includes={"foo": {"bar": None}})
    assert f1.merge(f2) == DatasetFilter(includes={"foo": {"bar": None}})

    f1 = DatasetFilter(excludes={"foo": None})
    f2 = DatasetFilter(excludes={"bar": None})
    assert f1.merge(f2) == DatasetFilter(excludes={"foo": None, "bar": None})

    f1 = DatasetFilter(excludes={"foo": None})
    f2 = DatasetFilter(excludes={"foo": {"bar": None}})
    assert f1.merge(f2) == DatasetFilter(excludes={"foo": {"bar": None}})

    f1 = DatasetFilter(
        includes={
            "SNOWFLAKE": None,
            "*": {"foo": None},
        }
    )
    f2 = DatasetFilter(includes={"*": {"bar": None}})
    assert f1.merge(f2) == DatasetFilter(
        includes={
            "SNOWFLAKE": None,
            "*": {"foo": None, "bar": None},
        }
    )

    f1 = DatasetFilter(
        excludes={
            "SNOWFLAKE": None,
            "*": {"foo": None},
        }
    )
    f2 = DatasetFilter(excludes={"*": {"bar": None}})
    assert f1.merge(f2) == DatasetFilter(
        excludes={
            "SNOWFLAKE": None,
            "*": {"foo": None, "bar": None},
        }
    )

    f1 = DatasetFilter(
        includes={
            "*": {"foo": {"f1", "f2"}, "bar": None},
        }
    )
    f2 = DatasetFilter(includes={"*": {"foo": {"f1", "f3"}, "bar": {"b1"}}})
    assert f1.merge(f2) == DatasetFilter(
        includes={
            "*": {
                "foo": {"f1", "f2", "f3"},
                "bar": {"b1"},
            },
        }
    )

    f1 = DatasetFilter(
        excludes={
            "x": {"foo": {"f1", "f2"}, "bar": None},
        }
    )
    f2 = DatasetFilter(excludes={"x": {"foo": {"f1", "f3"}, "bar": {"b1"}}})
    assert f1.merge(f2) == DatasetFilter(
        excludes={
            "x": {
                "foo": {"f1", "f2", "f3"},
                "bar": {"b1"},
            },
        }
    )


def test_include_database():
    # Includes only
    filter = DatasetFilter(includes={"*db": None, "test": {"schema1": ["*"]}})
    assert filter.include_database("foo_db")
    assert filter.include_database("TestDb")
    assert filter.include_database("TEST")
    assert not filter.include_database("app")

    # Excludes only
    filter = DatasetFilter(excludes={"foo": None, "bar": {"schame1": ["table1"]}})
    assert filter.include_database("test")
    assert filter.include_database("bar")
    assert not filter.include_database("foo")

    # Excludes take precedence over includes
    filter = DatasetFilter(includes={"foo*": None}, excludes={"foo_bar": None})
    assert filter.include_database("foo_baz")
    assert not filter.include_database("foo_bar")

    filter = DatasetFilter(includes={"*": None}, excludes={"foo_bar": None})
    assert filter.include_database("foo_baz")
    assert not filter.include_database("foo_bar")
