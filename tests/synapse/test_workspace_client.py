import copy
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from metaphor.synapse.model import (
    DedicatedSqlPoolSchema,
    DedicatedSqlPoolTable,
    QueryLogTable,
    SynapseTable,
    SynapseWorkspace,
    WorkspaceDatabase,
)
from metaphor.synapse.workspace_client import WorkspaceClient, pymssql

synapseWorkspace = SynapseWorkspace(
    id="mock_workspace_id/resourceGroups/mock_resource_group/providers/",
    name="mock_workspace_name",
    type="WORKSPACE",
    properties={
        "Origin": {"Type": "mock_database_type"},
        "connectivityEndpoints": {
            "dev": "mock_workspace_name-dev.synapse.net",
            "sql": "mock_workspace_name-dev.sql.synapse.net",
            "sqlOnDemand": "mock_workspace_name-dev-on-demand.sql.synapse.net",
        },
        "defaultDataLakeStorage": {
            "accountUrl": "mock_accunt_url.synapse.net",
            "filesystem": "mock_data_lake_storage",
        },
    },
)

workspaceDatabase = WorkspaceDatabase(
    id="mock_workspace_id/database/mock_database",
    name="mock_database",
    type="DATABASE",
    properties={},
)

workspaceClient = WorkspaceClient(synapseWorkspace, "mock_subscription_id", {}, {})


def test_get_database():
    with patch(
        "metaphor.synapse.workspace_client.get_request",
        return_value=[workspaceDatabase],
    ):
        dbs = workspaceClient.get_databases()
        assert len(dbs) == 1
        assert dbs[0] == workspaceDatabase


def test_get_dedicated_sql_pool_databases():
    with patch(
        "metaphor.synapse.workspace_client.get_request",
        return_value=[workspaceDatabase],
    ):
        dbs = workspaceClient.get_dedicated_sql_pool_databases()
        assert len(dbs) == 1
        assert dbs[0] == workspaceDatabase


def test_get_tables():
    table = SynapseTable(
        id="/tables/mock_table", name="mock_table", type="tables", properties={}
    )
    with patch("metaphor.synapse.workspace_client.get_request", return_value=[table]):
        tables = workspaceClient.get_tables("mock_database")
        assert len(tables) == 1
        assert tables[0] == table


def test_get_dedicated_sql_pool_tables():
    schema = DedicatedSqlPoolSchema(
        id="/schemas/mock_schema", name="mock_shcema", type="schemas"
    )

    table = DedicatedSqlPoolTable(
        id="/schemas/mock_schema/tables/mock_table", name="mock_table", type="tables"
    )

    columns = [
        {
            "id": "/schemas/mock_schema/tables/mock_table/columns/mock_col1",
            "name": "mock_col1",
            "type": "columns",
            "properties": {"columnType": "nvarchar"},
        },
        {
            "id": "/schemas/mock_schema/tables/mock_table/columns/mock_col2",
            "name": "mock_col2",
            "type": "columns",
            "properties": {"columnType": "bit"},
        },
    ]

    with patch(
        "metaphor.synapse.workspace_client.get_request",
        side_effect=[[schema], [table], columns],
    ):
        tables = workspaceClient.get_dedicated_sql_pool_tables("mock_database")

        verify_table = copy.deepcopy(table)
        verify_table.sqlSchema = schema
        verify_table.columns = columns

        assert len(tables) == 1
        assert tables[0] == verify_table


def test_get_sql_pool_query_log():
    querylog1 = QueryLogTable(
        request_id="test_request_id1",
        sql_query="SELECT TOP 10(*) FROM mock_table",
        login_name="mock_user1",
        start_time=QueryLogTable.to_utc_time((datetime.now() - timedelta(seconds=1))),
        end_time=QueryLogTable.to_utc_time(datetime.now()),
        duration=1000,
        query_size=10,
        error=None,
        query_operation="SELECT",
    )

    querylog2 = QueryLogTable(
        request_id="test_request_id2",
        sql_query="SELECT TOP 10(*) FROM mock_table",
        login_name="mock_user2",
        start_time=QueryLogTable.to_utc_time((datetime.now() - timedelta(seconds=2))),
        end_time=QueryLogTable.to_utc_time(datetime.now()),
        duration=2000,
        query_size=5,
        error=None,
        query_operation="SELECT",
    )

    rows = []
    for querylog in [querylog1, querylog2]:
        row = []
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

    conn_instance = MagicMock()
    cursor_instance = MagicMock()
    conn_instance.cursor = MagicMock(return_value=cursor_instance)
    cursor_instance.execute = MagicMock()
    cursor_instance.fetchall = MagicMock(return_value=rows)
    cursor_instance.close = MagicMock()
    conn_instance.close = MagicMock()

    with patch.object(pymssql, "connect", return_value=conn_instance):
        start_date = datetime.now() - timedelta(days=2)
        end_date = datetime.now()
        querylogs = workspaceClient.get_sql_pool_query_logs(
            "username", "password", start_date, end_date
        )
        assert next(querylogs) == querylog1
        assert next(querylogs) == querylog2


def test_get_dedicated_sql_pool_query_logs():
    querylog1 = QueryLogTable(
        request_id="test_request_id1",
        session_id="test_session_id1",
        sql_query="SELECT TOP 10(*) FROM mock_table",
        start_time=QueryLogTable.to_utc_time((datetime.now() - timedelta(seconds=1))),
        end_time=QueryLogTable.to_utc_time(datetime.now()),
        duration=1000,
        row_count=10,
        login_name="mock_user1",
        error=None,
        query_operation="SELECT",
    )

    querylog2 = QueryLogTable(
        request_id="test_request_id2",
        session_id="test_session_id2",
        sql_query="SELECT TOP 10(*) FROM mock_table",
        start_time=QueryLogTable.to_utc_time((datetime.now() - timedelta(seconds=3))),
        end_time=QueryLogTable.to_utc_time(datetime.now()),
        duration=3000,
        row_count=10,
        login_name="mock_user2",
        error=None,
        query_operation="SELECT",
    )

    rows = []
    for querylog in [querylog1, querylog2]:
        row = []
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

    conn_instance = MagicMock()
    cursor_instance = MagicMock()
    conn_instance.cursor = MagicMock(return_value=cursor_instance)
    cursor_instance.execute = MagicMock()
    cursor_instance.fetchall = MagicMock(return_value=rows)
    cursor_instance.close = MagicMock()
    conn_instance.close = MagicMock()

    with patch.object(pymssql, "connect", return_value=conn_instance):
        start_date = datetime.now() - timedelta(days=2)
        end_date = datetime.now()
        querylogs = workspaceClient.get_dedicated_sql_pool_query_logs(
            "database", "username", "password", start_date, end_date
        )
        assert next(querylogs) == querylog1
        assert next(querylogs) == querylog2
