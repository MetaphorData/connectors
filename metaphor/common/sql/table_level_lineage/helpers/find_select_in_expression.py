from typing import List, Set

import sqlglot.lineage
import sqlglot.optimizer.qualify
import sqlglot.optimizer.scope
from sqlglot import exp

from metaphor.common.logger import get_logger

logger = get_logger()


def _extract_select_from_insert(
    statement: exp.Insert,
) -> exp.Expression:
    if isinstance(statement.expression, exp.Values):
        # For INSERT INTO ... VALUES ..., just join all the
        # subqueries into a big select since this is just for
        # TLL
        select_statement = exp.Select().select("*")
        subqueries: List[exp.Subquery] = []
        values = statement.expression
        for tup in values.expressions:
            if isinstance(tup, exp.Tuple):
                for expr in tup.expressions:
                    if isinstance(expr, exp.Subquery):
                        subqueries.append(expr)
        for i, subquery in enumerate(subqueries):
            if not i:
                select_statement = select_statement.from_(
                    subquery.as_(f"_subquery_{i}")
                )
            else:
                select_statement = select_statement.join(subquery.as_(f"_subquery_{i}"))
        return select_statement

    if not isinstance(statement.expression, exp.Select):
        logger.warning(
            f"SELECT not directly in INSERT INTO, instead found {statement.expression.sql()}"
        )
    return statement.expression


_UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT: Set[str] = set(exp.Update.arg_types.keys()) - set(
    exp.Select.arg_types.keys()
)
_UPDATE_FROM_TABLE_ARGS_TO_MOVE = {"joins", "laterals", "pivot"}


def _extract_select_from_update(
    statement: exp.Update,
) -> exp.Select:
    statement = statement.copy()

    # The "SET" expressions need to be converted.
    # For the update command, it'll be a list of EQ expressions, but the select
    # should contain aliased columns.
    new_expressions = []
    for expr in statement.expressions:
        if isinstance(expr, exp.EQ) and isinstance(expr.left, exp.Column):
            new_expressions.append(
                exp.Alias(
                    this=expr.right,
                    alias=expr.left.this,
                )
            )
        else:
            # If we don't know how to convert it, just leave it as-is. If this causes issues,
            # they'll get caught later.
            new_expressions.append(expr)

    # Special translation for the `from` clause.
    extra_args = {}
    original_from = statement.args.get("from")
    if original_from and isinstance(original_from.this, exp.Table):
        # Move joins, laterals, and pivots from the Update->From->Table->field
        # to the top-level Select->field.

        for k in _UPDATE_FROM_TABLE_ARGS_TO_MOVE:
            if k in original_from.this.args:
                # Mutate the from table clause in-place.
                extra_args[k] = original_from.this.args.pop(k)

    select_statement = exp.Select(
        **{
            **{
                k: v
                for k, v in statement.args.items()
                if k not in _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT
            },
            **extra_args,
            "expressions": new_expressions,
        }
    )

    # Update statements always implicitly have the updated table in context.
    # TODO: Retain table name alias, if one was present.
    if select_statement.args.get("from"):
        select_statement = select_statement.join(
            statement.this, append=True, join_kind="cross"
        )
    else:
        select_statement = select_statement.from_(statement.this)

    return select_statement


def _extract_select_from_create(
    statement: exp.Create,
) -> exp.Expression:
    # TODO: Validate that this properly includes WITH clauses in all dialects.
    inner = statement.expression

    if inner:
        return inner
    else:
        return statement


def find_select_in_expression(expression: sqlglot.Expression) -> sqlglot.Expression:
    # Try to extract the core select logic from a more complex statement.
    # If it fails, just return the original statement.

    if isinstance(expression, exp.Merge):
        # TODO Need to map column renames in the expressions part of the statement.
        # Likely need to use the named_selects attr.
        expression = expression.args["using"]
        if isinstance(expression, exp.Table):
            # If we're querying a table directly, wrap it in a SELECT.
            expression = exp.Select().select("*").from_(expression)
    elif isinstance(expression, exp.Insert):
        # TODO Need to map column renames in the expressions part of the statement.
        expression = _extract_select_from_insert(expression)
    elif isinstance(expression, exp.Update):
        # Assumption: the output table is already captured in the modified tables list.
        expression = _extract_select_from_update(expression)
    elif isinstance(expression, exp.Create):
        # TODO May need to map column renames.
        # Assumption: the output table is already captured in the modified tables list.
        expression = _extract_select_from_create(expression)

    if isinstance(expression, exp.Subquery):
        expression = expression.unnest()

    return expression
