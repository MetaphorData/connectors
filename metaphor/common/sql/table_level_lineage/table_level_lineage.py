from collections import defaultdict
from typing import Dict, Optional, Set

from sqlglot import Expression, exp, maybe_parse
from sqlglot.optimizer.scope import build_scope

from metaphor.common.logger import get_logger
from metaphor.common.sql.dialect import PLATFORM_TO_DIALECT
from metaphor.common.sql.table_level_lineage.expression_handlers import (
    expression_handlers,
    find_target_in_select_into,
)
from metaphor.common.sql.table_level_lineage.find_select_in_expression import (
    find_select_in_expression,
)
from metaphor.common.sql.table_level_lineage.result import Result
from metaphor.common.sql.table_level_lineage.table import Table
from metaphor.models.metadata_change_event import DataPlatform

logger = get_logger()

_UPDATE_TYPES = {
    "CREATE": exp.Create,
    "INSERT": exp.Insert,
    "UPDATE": exp.Update,
    "MERGE": exp.Merge,
    "COPY": exp.Copy,
}


def _find_targets(expression: Expression) -> Set[Table]:
    targets: Set[Table] = set()

    for update_exp in expression.find_all(*_UPDATE_TYPES.values()):
        assert isinstance(update_exp, Expression)
        table = update_exp.find(exp.Table)
        if table:
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


def table_level_lineage(
    sql: str,
    platform: DataPlatform,
    account: Optional[str],
    statement_type: Optional[str],
) -> Result:

    if statement_type and statement_type.upper() not in _UPDATE_TYPES:
        # No target, no TLL possible
        return Result()

    try:
        expression: Expression = maybe_parse(
            sql,
            dialect=PLATFORM_TO_DIALECT[platform],
            into=_UPDATE_TYPES[statement_type] if statement_type else None,
        )
        return Result(
            targets=[
                target.to_queried_dataset(platform, account)
                for target in _find_targets(expression)
            ],
            sources=[
                source.to_queried_dataset(platform, account)
                for source in _find_sources(expression)
            ],
        )
    except Exception:
        logger.exception(f"Failed to parse table level lineage for query: {sql}")
        return Result()
