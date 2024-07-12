import re

_GET_AT = r"GET \'?@"
_DESCRIBE_EXTENDED_COLUMN_STATS = r"DESCRIBE EXTENDED [^ ]+ [^ ]+"
"""
This is for `DESCRIBE EXTENDED table_name column_name`.
"""


def is_bad_query_pattern(sql: str):
    """
    There are some sql expressions that SQLGlot cannot parse, but our data processing
    engine emits them. When we encounter these kinds of expressions, just ignore them.
    """
    for pattern in [
        _GET_AT,
        _DESCRIBE_EXTENDED_COLUMN_STATS,
    ]:
        if re.findall(pattern, sql):
            return True
    return False
