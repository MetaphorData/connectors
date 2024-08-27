from metaphor.postgresql.extractor import PostgreSQLExtractor


def test_parse_max_length():
    assert PostgreSQLExtractor._parse_max_length("foo") is None
    assert PostgreSQLExtractor._parse_max_length("foo(a)") is None
    assert PostgreSQLExtractor._parse_max_length("foo(10)") == 10


def test_parse_precision():
    assert PostgreSQLExtractor._parse_precision("foo") is None
    assert PostgreSQLExtractor._parse_precision("foo(a,10)") is None
    assert PostgreSQLExtractor._parse_precision("foo(,10)") is None
    assert PostgreSQLExtractor._parse_precision("foo(10,b)") is None
    assert PostgreSQLExtractor._parse_precision("foo(10,)") is None
    assert PostgreSQLExtractor._parse_precision("foo(10,3)") == 10
    assert PostgreSQLExtractor._parse_precision("foo(10)") == 10


def test_parse_format_type():
    assert PostgreSQLExtractor._parse_format_type("foo", "foo") == (None, None)
    assert PostgreSQLExtractor._parse_format_type("integer", "foo") == (32.0, None)
    assert PostgreSQLExtractor._parse_format_type("smallint", "foo") == (16.0, None)
    assert PostgreSQLExtractor._parse_format_type("bigint", "foo") == (64.0, None)
    assert PostgreSQLExtractor._parse_format_type("real", "foo") == (24.0, None)
    assert PostgreSQLExtractor._parse_format_type("double precision", "foo") == (
        53.0,
        None,
    )
    assert PostgreSQLExtractor._parse_format_type("numeric", "foo") == (None, None)
    assert PostgreSQLExtractor._parse_format_type("numeric", "numeric(10,1)") == (
        10,
        None,
    )
    assert PostgreSQLExtractor._parse_format_type("numeric", "numeric") == (None, None)
    assert PostgreSQLExtractor._parse_format_type("character", "character(10)") == (
        None,
        10,
    )
    assert PostgreSQLExtractor._parse_format_type(
        "character varying", "character varying(10)"
    ) == (None, 10)
