import typing as t
from typing import Optional

from sqlglot import Expression, exp, maybe_parse
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.generator import Generator

from metaphor.common.logger import get_logger
from metaphor.common.sql.dialect import PLATFORM_TO_DIALECT
from metaphor.common.sql.process_query.bad_queries import is_bad_query_pattern
from metaphor.common.sql.process_query.config import ProcessQueryConfig
from metaphor.common.sql.process_query.preprocess import preprocess
from metaphor.models.metadata_change_event import DataPlatform

logger = get_logger()


def _is_insert_values_into(expression: Expression) -> bool:
    return isinstance(expression, exp.Insert) and isinstance(
        expression.expression, exp.Values
    )


ALLOW_EXPRESSION_TYPES = (
    exp.Alter,
    exp.DDL,
    exp.DML,
    exp.Merge,
    exp.Query,
)


def process_query(
    sql: str,
    data_platform: DataPlatform,
    config: ProcessQueryConfig,
    query_id: Optional[str] = None,
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

    query_id : str
        The ID of the SQL query.

    Returns
    -------
    The processed SQL query as a `str`, or `None` if the SQL query does not
    include any lineage.
    """
    if not config.should_process:
        return sql

    if is_bad_query_pattern(sql):
        return sql

    dialect = PLATFORM_TO_DIALECT.get(data_platform)

    sqlglot_error_message = "Sqlglot failed to parse sql"
    if query_id:
        sqlglot_error_message += f", query id = {query_id}"

    try:
        updated = preprocess(sql, data_platform)
        expression: Expression = maybe_parse(updated, dialect=dialect)
    except SqlglotError as e:
        logger.debug(f"{sqlglot_error_message}: {e}")
        return None if config.skip_unparsable_queries else sql
    except RecursionError:
        logger.debug(f"{sqlglot_error_message}: maximum recursion depth exceeded")
        return None if config.skip_unparsable_queries else sql

    if config.ignore_insert_values_into and _is_insert_values_into(expression):
        return None

    if config.ignore_command_statement and isinstance(expression, exp.Command):
        return None

    if not isinstance(expression, ALLOW_EXPRESSION_TYPES):
        return None

    if not config.redact_literals.enabled:
        return updated

    DialectClass: t.Type[Dialect]
    if dialect is None:
        DialectClass = Dialect
    else:
        DialectClass = Dialect[dialect]
    GeneratorClass: t.Type[Generator] = DialectClass().generator().__class__

    # Mypy does not allow dynamic base classes, but that's the only way for us to do it
    class LiteralsRedacted(DialectClass):  # type: ignore
        class Generator(GeneratorClass):  # type: ignore
            TRANSFORMS: t.Dict[t.Type[exp.Expression], t.Callable[..., str]] = {
                **GeneratorClass.TRANSFORMS,
                exp.Literal: lambda *_: f"'{config.redact_literals.placeholder_literal}'",
            }

    return expression.sql(dialect=LiteralsRedacted)
