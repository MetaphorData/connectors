from queue import Queue
from typing import Any
from unittest.mock import MagicMock


def mock_sql_connection(
    fetch_all_side_effect: Any, execute_side_effect=None, mock_cursor=MagicMock()
) -> MagicMock:

    mock_cursor.execute = MagicMock()
    if execute_side_effect is not None:
        mock_cursor.execute.side_effect = execute_side_effect

    mock_cursor.fetchall = MagicMock()
    mock_cursor.fetchall.side_effect = fetch_all_side_effect

    mock_cursor_ctx = MagicMock()
    mock_cursor_ctx.__enter__ = MagicMock()
    mock_cursor_ctx.__enter__.return_value = mock_cursor

    mock_connection = MagicMock()
    mock_connection.cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor_ctx

    return mock_connection


def mock_connection_pool(
    fetch_all_side_effect: Any, execute_side_effect=None, mock_cursor=MagicMock()
) -> Queue:

    mock_connection_pool: Queue = Queue(1)
    mock_connection_pool.put(
        mock_sql_connection(fetch_all_side_effect, execute_side_effect, mock_cursor)
    )

    return mock_connection_pool
