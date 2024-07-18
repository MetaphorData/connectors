from typing import Optional

from sqlglot import Expression, exp, maybe_parse
from sqlglot.errors import SqlglotError

from metaphor.common.logger import get_logger
from metaphor.common.process_query.bad_queries import is_bad_query_pattern
from metaphor.common.process_query.config import ProcessQueryConfig
from metaphor.common.process_query.preprocess import preprocess
from metaphor.models.metadata_change_event import DataPlatform

logger = get_logger()

_PLATFORM_TO_DIALECT = {
    DataPlatform.BIGQUERY: "bigquery",
    DataPlatform.HIVE: "hive",
    DataPlatform.MSSQL: "tsql",
    DataPlatform.MYSQL: "mysql",
    DataPlatform.POSTGRESQL: "postgres",
    DataPlatform.REDSHIFT: "redshift",
    DataPlatform.SNOWFLAKE: "snowflake",
    DataPlatform.UNITY_CATALOG: "databricks",
}


def _redact_literal_values_in_where_clauses(
    expression: Expression, config: ProcessQueryConfig
) -> None:
    for where in expression.find_all(exp.Where):
        for lit in where.find_all(exp.Literal):
            lit.args["this"] = config.redact_literals.placeholder_literal


def _redact_literal_values_in_when_not_matched_insert_clauses(
    expression: Expression,
    config: ProcessQueryConfig,
) -> None:
    for when in expression.find_all(exp.When):
        if "matched" not in when.args or when.args["matched"]:
            continue
        if not isinstance(when.args.get("then"), exp.Insert):
            continue

        values = when.args["then"].expression
        if not isinstance(values, exp.Tuple):
            continue

        for lit in values.find_all(exp.Literal):
            lit.args["this"] = config.redact_literals.placeholder_literal


def _is_insert_values_into(expression: Expression) -> bool:
    return isinstance(expression, exp.Insert) and isinstance(
        expression.expression, exp.Values
    )


def process_query(
    sql: str,
    data_platform: DataPlatform,
    config: ProcessQueryConfig,
) -> Optional[str]:
    """
    Processes a crawled SQL query.

    Parameters
    ----------
    sql : str
        The SQL query to process.

    data_platform : DataPlatform
        The data platform. If not supported, the query will not be processed.

    config : ProcessQueryConfig
        Config for controlling what to do.

    Returns
    -------
    The processed SQL query as a `str`, or `None` if the SQL query does not
    include any lineage.
    """
    if not config.should_process:
        return sql

    if is_bad_query_pattern(sql):
        return sql

    dialect = _PLATFORM_TO_DIALECT.get(data_platform)

    try:
        updated = preprocess(sql, data_platform)
        expression: Expression = maybe_parse(updated, dialect=dialect)
    except SqlglotError:
        logger.warning(f"Failed to parse sql: {sql}")
        return sql

    if config.ignore_insert_values_into and _is_insert_values_into(expression):
        return None

    if config.redact_literals.where_clauses:
        _redact_literal_values_in_where_clauses(expression, config)

    if config.redact_literals.when_not_matched_insert_clauses:
        _redact_literal_values_in_when_not_matched_insert_clauses(expression, config)

    return expression.sql(dialect=dialect)
