import logging
import time
from concurrent import futures
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional, Set, Tuple

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from metaphor.models.metadata_change_event import (
    MaterializationType,
    SnowflakeStreamSourceType,
    SnowflakeStreamType,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_THREAD_POOL_SIZE = 10
DEFAULT_SLEEP_TIME = 0.1  # 0.1 s


class SnowflakeTableType(Enum):
    """
    All except for `STREAM` are table types returned by the information schema. See `Columns` section in https://docs.snowflake.com/en/sql-reference/info-schema/tables for all possible table types in infomation schema.

    `STREAM` is there because information schema doesn't know about this type.
    """

    BASE_TABLE = "BASE TABLE"
    VIEW = "VIEW"
    TEMPORARY_TABLE = "TEMPORARY TABLE"
    EXTERNAL_TABLE = "EXTERNAL TABLE"
    EVENT_TABLE = "EVENT TABLE"
    MATERIALIZED_VIEW = "MATERIALIZED VIEW"
    STREAM = "STREAM"


table_type_to_materialization_type: Dict[SnowflakeTableType, MaterializationType] = {
    SnowflakeTableType.VIEW: MaterializationType.VIEW,
    SnowflakeTableType.STREAM: MaterializationType.STREAM,
    SnowflakeTableType.BASE_TABLE: MaterializationType.TABLE,
    SnowflakeTableType.TEMPORARY_TABLE: MaterializationType.TABLE,
    SnowflakeTableType.EVENT_TABLE: MaterializationType.TABLE,
    SnowflakeTableType.EXTERNAL_TABLE: MaterializationType.TABLE,
    SnowflakeTableType.MATERIALIZED_VIEW: MaterializationType.MATERIALIZED_VIEW,
}

str_to_source_type: Dict[str, SnowflakeStreamSourceType] = {
    "TABLE": SnowflakeStreamSourceType.TABLE,
    "VIEW": SnowflakeStreamSourceType.VIEW,
    "EXTERNAL_TABLE": SnowflakeStreamSourceType.TABLE,
    "DIRECTORY_TABLE": SnowflakeStreamSourceType.TABLE,
}

str_to_stream_type: Dict[str, SnowflakeStreamType] = {
    "DEFAULT": SnowflakeStreamType.STANDARD,
    "APPEND_ONLY": SnowflakeStreamType.APPEND_ONLY,
    "INSERT_ONLY": SnowflakeStreamType.INSERT_ONLY,
}


@dataclass
class DatasetInfo:
    database: str
    schema: str
    name: str
    type: str
    row_count: Optional[int] = None


@dataclass
class QueryWithParam:
    query: str
    params: Optional[Tuple] = None


def to_quoted_identifier(parts: List[Optional[str]]) -> str:
    """Converts the parts into a dot joined and quoted snowflake identifier"""
    return ".".join([f"""\"{part.replace('"', '""')}\"""" for part in parts if part])


def async_query(conn: SnowflakeConnection, query: QueryWithParam) -> SnowflakeCursor:
    """Executing a snowflake query asynchronously"""
    cursor = conn.cursor()
    if query.params is not None:
        logger.debug(f"Query {query.query} params {query.params}")
        cursor.execute_async(query.query, query.params)
    else:
        cursor.execute_async(query.query)

    query_id = cursor.sfqid
    assert query_id, "Invalid query id None"

    # Wait for the query to finish running.
    while conn.is_still_running(conn.get_query_status(query_id)):
        time.sleep(DEFAULT_SLEEP_TIME)

    cursor.get_results_from_sfqid(query_id)
    return cursor


def async_execute(
    conn: SnowflakeConnection,
    queries: Dict[str, QueryWithParam],
    query_name: str = "",
    max_workers: Optional[int] = None,
    results_processor: Optional[Callable[[str, List], None]] = None,
) -> Dict[str, List]:
    """
    Executing snowflake query with a set of parameters using thread pool
    If results_processor is not provided, will return Dict[key, result_tuples],
    Otherwise, apply the results_processor to the result_tuples
    """
    workers = max_workers if max_workers is not None else DEFAULT_THREAD_POOL_SIZE
    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(async_query, conn, query): key
            for key, query in queries.items()
        }

        results_map = {}
        for future in futures.as_completed(future_map):
            key = future_map[future]
            try:
                results = future.result().fetchall()
            except Exception:
                logger.exception(f"Error executing {query_name} for {key}")
                continue

            if results_processor is None:
                results_map[key] = results
            else:
                results_processor(key, results)

        return results_map


def exclude_username_clause(excluded_usernames: Set[str]) -> str:
    """
    Excludes usernames from query history output
    use "q" as "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY" table alias
    """
    return (
        f"and q.USER_NAME NOT IN ({','.join(['%s'] * len(excluded_usernames))})"
        if len(excluded_usernames) > 0
        else ""
    )


def check_access_history(
    conn: SnowflakeConnection,
) -> bool:
    """
    Check if access history table is available
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT QUERY_ID
        FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
        LIMIT 1
        """
    )
    result = cursor.fetchall()
    return len(result) > 0


def fetch_query_history_count(
    conn: SnowflakeConnection,
    start_date: datetime,
    excluded_usernames: Set[str],
    end_date: datetime = datetime.now(),
    has_access_history: bool = True,
) -> int:
    """
    Fetch query history count
    """
    cursor = conn.cursor()
    if has_access_history:
        cursor.execute(
            f"""
            SELECT COUNT(1)
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
            JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
              ON a.QUERY_ID = q.QUERY_ID
            WHERE EXECUTION_STATUS = 'SUCCESS'
              and START_TIME > %s and START_TIME <= %s
              and QUERY_START_TIME > %s AND QUERY_START_TIME <= %s
              {exclude_username_clause(excluded_usernames)}
            """,
            (
                start_date,
                end_date,
                start_date,
                end_date,
                *excluded_usernames,
            ),
        )
    else:
        cursor.execute(
            f"""
            SELECT COUNT(1)
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
            WHERE EXECUTION_STATUS = 'SUCCESS'
              and START_TIME > %s and START_TIME <= %s
              {exclude_username_clause(excluded_usernames)}
            """,
            (
                start_date,
                end_date,
                *excluded_usernames,
            ),
        )

    result = cursor.fetchone()
    if result is not None:
        return result[0]
    return 0
