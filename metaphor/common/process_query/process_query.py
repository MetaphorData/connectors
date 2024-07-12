from sqlglot import Expression, exp, maybe_parse
from sqlglot.errors import SqlglotError

from metaphor.common.logger import get_logger
from metaphor.common.process_query.bad_queries import is_bad_query_pattern
from metaphor.common.process_query.config import ProcessQueryConfig
from metaphor.common.process_query.preprocess import preprocess
from metaphor.models.metadata_change_event import DataPlatform

logger = get_logger()

SUPPORTED_DIALECTS = {
    DataPlatform.SNOWFLAKE: "snowflake",
}

_REDACTED_LITERAL_VALUE = "__REDACTED_BY_METAPHOR__"


def _redact_literal_values_in_where_clauses(expression: Expression) -> None:
    for where in expression.find_all(exp.Where):
        for lit in where.find_all(exp.Literal):
            lit.args["this"] = _REDACTED_LITERAL_VALUE


def process_query(
    sql: str,
    data_platform: DataPlatform,
    config: ProcessQueryConfig,
) -> str:
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
    the processed SQL query as a `str`.
    """
    if not config.should_process:
        return sql

    if is_bad_query_pattern(sql):
        return sql

    dialect = SUPPORTED_DIALECTS.get(data_platform)
    if not dialect:
        return sql

    try:
        updated = preprocess(sql, data_platform)
        expression: Expression = maybe_parse(updated, dialect=dialect)
    except SqlglotError:
        logger.warning(f"Cannot parse sql with sqlglot: {sql}")
        return sql

    if config.redact_literal_values_in_where_clauses:
        _redact_literal_values_in_where_clauses(expression)

    return expression.sql(dialect=dialect)
