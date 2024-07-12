import re


def drop_snowflake_copy_into_options(sql: str) -> str:
    sql = sql.strip()
    pat = r"^\s?copy into"
    match = next(re.finditer(pat, sql, re.IGNORECASE), None)
    if not match:
        # Not a COPY INTO, don't preprocess
        return sql

    patterns = [
        r"files\s?=\s?\([^\)]+\)",
        r"region\s?=\s?'[^']+'",
        r"credentials\s?=\s?\([^\)]+\)",
        r"encryption\s?=\s?\([^\)]+\)",
        r"file_format\s?=\s?\([^\)]+\)",
    ]

    for pattern in patterns:
        sql = re.sub(pattern, "", sql, count=1, flags=re.IGNORECASE)

    # Reference: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#copy-options-copyoptions
    copy_options = [
        "PURGE",
        "ON_ERROR",
        "SIZE_LIMIT",
        "RETURN_FAILED_ONLY",
        "MATCH_BY_COLUMN_NAME",
        "INCLUDE_METADATA",
        "ENFORCE_LENGTH",
        "TRUNCATECOLUMNS",
        "FORCE",
        "LOAD_UNCERTAIN_FILES",
        "STORAGE_INTEGRATION",
        "FILE_FORMAT",
        "MAX_FILE_SIZE",
    ]

    for copy_option in copy_options:
        copy_option_pat = (
            copy_option + r"\s?=\s?[^(\s,>)]+"
        )  # `file_format => ...` can be parsed, we don't want to match that
        sql = re.sub(copy_option_pat, "", sql, count=1, flags=re.IGNORECASE)
    return sql
