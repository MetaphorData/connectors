import logging
import re
from collections import defaultdict
from typing import Dict, Optional, Set

import sqlglot
import sqlglot.errors
from sqlglot import Expression, exp, maybe_parse
from sqlglot.optimizer.scope import build_scope

from metaphor.common.logger import get_logger
from metaphor.common.sql.dialect import PLATFORM_TO_DIALECT
from metaphor.common.sql.table_level_lineage.helpers.expression_handlers import (
    expression_handlers,
    find_target_in_select_into,
)
from metaphor.common.sql.table_level_lineage.helpers.find_select_in_expression import (
    find_select_in_expression,
)
from metaphor.common.sql.table_level_lineage.result import Result
from metaphor.common.sql.table_level_lineage.table import Table
from metaphor.models.metadata_change_event import DataPlatform

logger = get_logger()


class IgnoreSQLGlotUnsupportedSyntax(logging.Filter):
    def filter(self, record):
        return (
            "contains unsupported syntax. Falling back to parsing as a 'Command'"
            not in record.getMessage()
        )


def _is_truncated_insert_into_with_values(sql: str):
    sql = re.sub(r"/\*.*?\*/\s*", "", sql, flags=re.DOTALL)
    match = re.match(r"^insert\s+into[^\(]+\([^\)]+\)\s+values", sql, re.IGNORECASE)
    return match is not None and sql.endswith("...")


sqlglot_logger = logging.getLogger("sqlglot")
sqlglot_logger.addFilter(IgnoreSQLGlotUnsupportedSyntax())

_UPDATE_TYPES = (
    exp.Create,
    exp.Insert,
    exp.Update,
    exp.Merge,
    exp.Copy,
)

_VALID_STATEMENT_TYPES = {
    "CREATE",
    "INSERT",
    "UPDATE",
    "MERGE",
    "COPY",
}


def _find_targets(expression: Expression) -> Set[Table]:
    targets: Set[Table] = set()

    for update_exp in expression.find_all(*_UPDATE_TYPES):
        assert isinstance(update_exp, Expression)
        table = update_exp.find(exp.Table)
        if table:
            if isinstance(update_exp, exp.Create) and update_exp.kind not in {
                "TABLE",
                "VIEW",
            }:
                # This is not really updating a table, ignore this
                continue
            targets.add(Table.from_sqlglot_table(table))

    # If there's a `SELECT INTO`, we want to include the target table as well.
    targets.update(find_target_in_select_into(expression))

    return targets


def _find_upstream_tables(expression: Expression):
    # See if this expression is something we need to parse ourselves,
    # if it is and we can find the sources, these are the TLL source
    # tables.
    for handler in expression_handlers:
        res = handler(expression)
        if res is not None:
            return res

    sources: Set[Table] = set()
    root = build_scope(expression)
    if root is None:
        return sources

    for scope in root.traverse():
        for _, (_, source) in scope.selected_sources.items():
            if isinstance(source, exp.Table):
                sources.add(Table.from_sqlglot_table(source))

    return sources


def _find_sources(expression: Expression) -> Set[Table]:

    cte_sources: Dict[str, Set[Table]] = defaultdict(set)

    for cte in expression.find_all(exp.CTE):
        # cte has to have an alias name
        table_alias = cte.find(exp.TableAlias)
        assert table_alias is not None
        for cte_source_info in _find_upstream_tables(cte.this):
            if cte_source_info.table in cte_sources:
                cte_sources[table_alias.alias_or_name].update(
                    cte_sources[cte_source_info.table]
                )
            else:
                cte_sources[table_alias.alias_or_name].add(cte_source_info)

    sources: Set[Table] = set()
    query_sources = _find_upstream_tables(find_select_in_expression(expression))
    for query_source in query_sources:
        if query_source.table in cte_sources:
            sources.update(cte_sources[query_source.table])
        else:
            sources.add(query_source)

    return sources


def extract_table_level_lineage(
    sql: str,
    platform: DataPlatform,
    account: Optional[str],
    statement_type: Optional[str] = None,
    query_id: Optional[str] = None,
    default_database: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> Result:

    if statement_type and statement_type.upper() not in _VALID_STATEMENT_TYPES:
        # No target, no TLL possible
        return Result()

    try:
        expression: Expression = maybe_parse(
            sql, dialect=PLATFORM_TO_DIALECT.get(platform)
        )
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        if not _is_truncated_insert_into_with_values(sql) and query_id:
            logger.warning(f"Cannot parse sql with SQLGlot, query_id = {query_id}")
        return Result()
    except RecursionError:
        if query_id:
            logger.warning(
                f"Cannot parse sql with SQLGlot (max recursion level exceeded), query_id = {query_id}"
            )
        return Result()

    try:
        return Result(
            targets=[
                target.to_queried_dataset(
                    platform, account, default_database, default_schema
                )
                for target in _find_targets(expression)
            ],
            sources=[
                source.to_queried_dataset(
                    platform, account, default_database, default_schema
                )
                for source in _find_sources(expression)
            ],
        )
    except Exception:
        logger.exception(
            f"Failed to parse table level lineage for query, query_id = {query_id}"
        )
        return Result()
