from datetime import datetime, timedelta
from typing import Any, List
from unittest.mock import MagicMock, patch

from metaphor.common.utils import to_utc_time
from metaphor.synapse.model import SynapseQueryLog
from metaphor.synapse.workspace_query import MssqlConnectConfig, WorkspaceQuery


@patch("metaphor.synapse.workspace_query.mssql_fetch_all")
def test_get_sql_pool_query_log(mock_mssql_fetch_all: MagicMock):
    querylog1 = SynapseQueryLog(
        request_id="test_request_id1",
        sql_query="SELECT TOP 10(*) FROM mock_table",
        login_name="mock_user1",
        start_time=to_utc_time((datetime.now() - timedelta(seconds=1))),
        end_time=to_utc_time(datetime.now()),
        duration=1000,
        query_size=10,
        error=None,
        query_operation="SELECT",
    )

    querylog2 = SynapseQueryLog(
        request_id="test_request_id2",
        sql_query="SELECT TOP 10(*) FROM mock_table",
        login_name="mock_user2",
        start_time=to_utc_time((datetime.now() - timedelta(seconds=2))),
        end_time=to_utc_time(datetime.now()),
        duration=2000,
        query_size=5,
        error=None,
        query_operation="SELECT",
    )

    rows: List[List[Any]] = []
    for querylog in [querylog1, querylog2]:
        row: List[Any] = []
        row.append(querylog.request_id)
        row.append(querylog.sql_query)
        row.append(querylog.login_name)
        row.append(querylog.start_time)
        row.append(querylog.end_time)
        row.append(querylog.duration)
        row.append(querylog.query_size)
        row.append(querylog.error)
        row.append(querylog.query_operation)
        rows.append(row)

    mock_mssql_fetch_all.return_value = rows

    config = MssqlConnectConfig(
        endpoint="mock-server-ondemand.sql.azuresynapse.net",
        username="mock_username",
        password="mock_password",
    )
    start_date = datetime.now() - timedelta(days=2)
    end_date = datetime.now()
    querylogs = WorkspaceQuery.get_sql_pool_query_logs(config, start_date, end_date)
    assert next(querylogs) == querylog1
    assert next(querylogs) == querylog2


@patch("metaphor.synapse.workspace_query.mssql_fetch_all")
def test_get_dedicated_sql_pool_query_logs(mock_mssql_fetch_all: MagicMock):
    querylog1 = SynapseQueryLog(
        request_id="test_request_id1",
        session_id="test_session_id1",
        sql_query="SELECT TOP 10(*) FROM mock_table",
        start_time=to_utc_time((datetime.now() - timedelta(seconds=1))),
        end_time=to_utc_time(datetime.now()),
        duration=1000,
        row_count=10,
        login_name="mock_user1",
        error=None,
        query_operation="SELECT",
    )

    querylog2 = SynapseQueryLog(
        request_id="test_request_id2",
        session_id="test_session_id2",
        sql_query="SELECT TOP 10(*) FROM mock_table",
        start_time=to_utc_time((datetime.now() - timedelta(seconds=3))),
        end_time=to_utc_time(datetime.now()),
        duration=3000,
        row_count=10,
        login_name="mock_user2",
        error=None,
        query_operation="SELECT",
    )

    rows: List[List[Any]] = []
    for querylog in [querylog1, querylog2]:
        row: List[Any] = []
        row.append(querylog.request_id)
        row.append(querylog.session_id)
        row.append(querylog.sql_query)
        row.append(querylog.start_time)
        row.append(querylog.end_time)
        row.append(querylog.duration)
        row.append(querylog.row_count)
        row.append(querylog.login_name)
        row.append(querylog.error)
        row.append(querylog.query_operation)
        rows.append(row)

    mock_mssql_fetch_all.return_value = rows

    config = MssqlConnectConfig(
        endpoint="mock-server.sql.azuresynapse.net",
        username="mock_username",
        password="mock_password",
    )
    start_date = datetime.now() - timedelta(days=2)
    end_date = datetime.now()
    querylogs = WorkspaceQuery.get_dedicated_sql_pool_query_logs(
        config, "database", start_date, end_date
    )
    assert next(querylogs) == querylog1
    assert next(querylogs) == querylog2
