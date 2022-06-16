import logging
import time
from concurrent import futures
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_THREAD_POOL_SIZE = 10
DEFAULT_SLEEP_TIME = 0.1  # 0.1 s


class SnowflakeTableType(Enum):
    BASE_TABLE = "BASE TABLE"
    VIEW = "VIEW"
    TEMPORARY_TABLE = "TEMPORARY TABLE"


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


def async_query(conn: SnowflakeConnection, query: QueryWithParam) -> SnowflakeCursor:
    """Executing a snowflake query asynchronously"""
    cursor = conn.cursor()
    if query.params is not None:
        logger.debug(f"Query {query.query} params {query.params}")
        cursor.execute_async(query.query, query.params)
    else:
        cursor.execute_async(query.query)

    query_id = cursor.sfqid

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


def fetch_query_history_count(
    conn: SnowflakeConnection,
    start_date: datetime,
    excluded_usernames: List[str],
) -> int:
    """
    Fetch query history count
    """
    excluded_usernames_clause = (
        f"and USER_NAME NOT IN ({','.join(['%s'] * len(excluded_usernames))})"
        if len(excluded_usernames) > 0
        else ""
    )

    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT COUNT(1)
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE EXECUTION_STATUS = 'SUCCESS' and START_TIME > %s
          {excluded_usernames_clause}
        """,
        (
            start_date,
            *excluded_usernames,
        ),
    )
    result = cursor.fetchone()
    if result is not None:
        return result[0]
    return 0
