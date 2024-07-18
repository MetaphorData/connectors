from metaphor.common.process_query import process_query
from metaphor.common.process_query.config import ProcessQueryConfig
from metaphor.models.metadata_change_event import DataPlatform

config = ProcessQueryConfig(
    redact_literal_values_in_where_clauses=True, ignore_insert_values_into=True
)


def test_redact_literal_values_in_where_clauses():
    sql = "SELECT col FROM src WHERE col > 12345 AND col < 99999"
    processed = process_query(sql, DataPlatform.SNOWFLAKE, config)
    assert (
        processed
        == f"SELECT col FROM src WHERE col > {config.redacted_literal_placeholder} AND col < {config.redacted_literal_placeholder}"
    )

    sql = """
    WITH q AS (
        SELECT
            *
        FROM (
            SELECT
                foo,
                bar
            FROM (
                SELECT
                    *
                FROM
                    db.sch.src
                WHERE
                    col > 1234 AND col < 9999
            )
            WHERE
                col2 == 'some value'
        )
        WHERE
            foo == 'not really foo'
    )
    SELECT
        foo,
        bar
    FROM
        q
    WHERE
        bar != 'this cannot be bar'
    """
    processed = process_query(sql, DataPlatform.SNOWFLAKE, config)
    # Notice `==` becomes `=` and `!=` becomes `<>`
    expected = "WITH q AS (SELECT * FROM (SELECT foo, bar FROM (SELECT * FROM db.sch.src WHERE col > <REDACTED> AND col < <REDACTED>) WHERE col2 = '<REDACTED>') WHERE foo = '<REDACTED>') SELECT foo, bar FROM q WHERE bar <> '<REDACTED>'"
    assert processed == expected


def test_handle_snowflake_copy_into():
    sql = """
    COPY INTO tgt
(
    foo,
    bar
)
FROM
    (
        SELECT
            *
        FROM
            src
    )
FILES = ('1') REGION = 'us-east-1' CREDENTIALS = (AWS_KEY_ID = 'id' AWS_SECRET_KEY = 'key' AWS_TOKEN='tok') ENCRYPTION = (MASTER_KEY = '') FILE_FORMAT = ( BINARY_FORMAT = BASE64 TYPE = 'csv' ESCAPE = NONE ESCAPE_UNENCLOSED_FIELD = NONE FIELD_OPTIONALLY_ENCLOSED_BY = '\"' COMPRESSION = 'zstd' NULL_IF = 'null-vba3aoqjpgeovgjvmzn5cklcstanclr' SKIP_HEADER = 1 ) PURGE = TRUE ON_ERROR = abort_statement
    """
    processed = process_query(
        sql,
        DataPlatform.SNOWFLAKE,
        config,
    )
    assert processed == "COPY INTO tgt (foo, bar) FROM (SELECT * FROM src)"


def test_scrub_literals_in_where_in():
    sql = """
    SELECT p.FirstName, p.LastName, e.JobTitle
FROM Person.Person AS p
JOIN HumanResources.Employee AS e
    ON p.BusinessEntityID = e.BusinessEntityID
WHERE e.JobTitle IN ('Design Engineer', 'Tool Designer', 'Marketing Assistant')
    """
    processed = process_query(
        sql,
        DataPlatform.MSSQL,
        config,
    )
    assert (
        processed
        == "SELECT p.FirstName, p.LastName, e.JobTitle FROM Person.Person AS p JOIN HumanResources.Employee AS e ON p.BusinessEntityID = e.BusinessEntityID WHERE e.JobTitle IN ('<REDACTED>', '<REDACTED>', '<REDACTED>')"
    )


def test_scrub_literals_in_where_or():
    sql = """
SELECT p.FirstName, p.LastName, e.JobTitle
FROM Person.Person AS p
JOIN HumanResources.Employee AS e
    ON p.BusinessEntityID = e.BusinessEntityID
WHERE e.JobTitle = 'Design Engineer'
   OR e.JobTitle = 'Tool Designer'
   OR e.JobTitle = 'Marketing Assistant'
    """
    processed = process_query(
        sql,
        DataPlatform.MSSQL,
        config,
    )
    assert (
        processed
        == "SELECT p.FirstName, p.LastName, e.JobTitle FROM Person.Person AS p JOIN HumanResources.Employee AS e ON p.BusinessEntityID = e.BusinessEntityID WHERE e.JobTitle = '<REDACTED>' OR e.JobTitle = '<REDACTED>' OR e.JobTitle = '<REDACTED>'"
    )


def test_ignore_insert_value_into():
    sql = """
INSERT INTO employees (id, name, age)
VALUES
(1, 'John Doe', 30),
(2, 'Jane Smith', 25),
(3, 'Bob Johnson', 40);
    """
    processed = process_query(
        sql,
        DataPlatform.SNOWFLAKE,
        config,
    )
    assert processed is None
