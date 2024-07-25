from typing import Optional, Set

from sqlglot import Expression, exp

from metaphor.common.sql.table_level_lineage.table import Table


def _handle_create_like(
    expression: Expression,
) -> Optional[Set[Table]]:
    if not isinstance(expression, exp.Create):
        return None
    if "properties" not in expression.args:
        return None
    props = expression.args["properties"]
    if not isinstance(props, exp.Properties):
        return None
    if not len(props.expressions) == 1:
        return None
    prop = props.expressions[0]
    if not isinstance(prop, exp.LikeProperty) or not isinstance(prop.this, exp.Table):
        return None
    return {Table.from_sqlglot_table(prop.this)}


def _handle_create_clone(
    expression: Expression,
) -> Optional[Set[Table]]:
    if not isinstance(expression, exp.Create):
        return None
    if "clone" not in expression.args:
        return None
    clone = expression.args["clone"]
    if not isinstance(clone, exp.Clone):
        return None
    if not isinstance(clone.this, exp.Table):
        return None
    return {Table.from_sqlglot_table(clone.this)}


def find_target_in_select_into(
    expression: Expression,
) -> Set[Table]:
    res: Set[Table] = set()
    for select in expression.find_all(exp.Select):
        if "into" not in select.args:
            continue
        into = select.args["into"]
        if not isinstance(into, exp.Into) or not isinstance(into.this, exp.Table):
            continue
        res.add(Table.from_sqlglot_table(into.this))
    return res


expression_handlers = [
    _handle_create_clone,
    _handle_create_like,
]
"""
Some expressions don't have SELECT in them but they still have
lineage that we want to know. We parse these expressions separately.
"""
