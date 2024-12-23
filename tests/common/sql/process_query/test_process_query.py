from pathlib import Path

import pytest
import sqlglot

from metaphor.common.sql.dialect import PLATFORM_TO_DIALECT
from metaphor.common.sql.process_query import process_query
from metaphor.common.sql.process_query.config import (
    ProcessQueryConfig,
    RedactPIILiteralsConfig,
)
from metaphor.models.metadata_change_event import DataPlatform

config = ProcessQueryConfig(
    redact_literals=RedactPIILiteralsConfig(
        enabled=True,
    ),
    ignore_insert_values_into=True,
    ignore_command_statement=True,
)

pre_preprocess_only = ProcessQueryConfig(ignore_command_statement=True)


@pytest.mark.parametrize(
    ["name", "platform", "config"],
    [
        ("redact_literal_values_in_where_clauses", DataPlatform.SNOWFLAKE, config),
        ("redact_simple", DataPlatform.SNOWFLAKE, config),
        ("snowflake_copy_into", DataPlatform.SNOWFLAKE, config),
        ("snowflake_copy_into", DataPlatform.SNOWFLAKE, pre_preprocess_only),
        ("redact_literals_in_where_in", DataPlatform.MSSQL, config),
        ("redact_literals_in_where_or", DataPlatform.MSSQL, config),
        ("redact_merge_insert_when_not_matched", DataPlatform.MSSQL, config),
    ],
)
def test_process_query(name: str, platform: DataPlatform, config: ProcessQueryConfig):
    dir = Path(__file__).parent / "process_query" / name
    with open(dir / "query.sql") as f:
        sql = f.read()
    processed = process_query(sql, platform, config)
    assert processed
    dialect = PLATFORM_TO_DIALECT.get(platform)
    actual: sqlglot.Expression = sqlglot.maybe_parse(processed, dialect=dialect)
    with open(dir / "expected.sql") as f:
        expected: sqlglot.Expression = sqlglot.maybe_parse(f.read(), dialect=dialect)
    assert actual.sql(dialect=dialect) == expected.sql(dialect=dialect)


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


def test_skip_unparseable_queries():
    sql = """
    SELECT
        { fn convert("D", SQL_DATE) } as DATE
    FROM
        db.sch.tab
    """

    # This skips the unparseable query
    processed = process_query(
        sql,
        DataPlatform.SNOWFLAKE,
        ProcessQueryConfig(
            redact_literals=RedactPIILiteralsConfig(
                enabled=True,
            ),
            skip_unparsable_queries=True,
        ),
    )
    assert processed is None

    # This passes through
    processed = process_query(
        sql,
        DataPlatform.SNOWFLAKE,
        config,
    )
    assert processed == sql


def test_ignore_rollback():
    assert (
        process_query(
            "ROLLBACK;",
            DataPlatform.MYSQL,
            config,
        )
        is None
    )


def test_ignore_show_exp():
    assert (
        process_query(
            "SHOW SCHEMAS;",
            DataPlatform.MYSQL,
            config,
        )
        is None
    )


def test_ignore_set_exp():
    assert (
        process_query(
            "SET NAMES utf8mb4",
            DataPlatform.MYSQL,
            config,
        )
        is None
    )
