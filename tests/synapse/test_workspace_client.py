from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from metaphor.common.utils import to_utc_time
from metaphor.synapse.model import (
    SynapseColumn,
    SynapseDatabase,
    SynapseQueryLog,
    SynapseTable,
)
from metaphor.synapse.workspace_client import WorkspaceClient, pymssql

synapseDatabase = SynapseDatabase(
    id=1,
    name="mock_database_name",
    create_time=to_utc_time(datetime.now()),
    collation_name="Latin1_General_100_CI_AS_SC_UTF8",
)

synapseColumn1 = SynapseColumn(
    name="mock_column1",
    type="bigint",
    max_length=8.0,
    precision=19.0,
    is_nullable=False,
    is_unique=True,
    is_primary_key=True,
    is_foreign_key=False,
)

synapseColumn2 = SynapseColumn(
    name="mock_column2",
    type="varchar",
    max_length=256.0,
    precision=0.0,
    is_nullable=True,
    is_primary_key=False,
)

workspaceClient = WorkspaceClient("mock_workspace_name", "username", "password")


def test_get_database():
    rows = [
        [
            synapseDatabase.id,
            synapseDatabase.name,
            synapseDatabase.create_time,
            synapseDatabase.collation_name,
        ]
    ]
    with patch(
        "metaphor.synapse.workspace_client.WorkspaceClient._sql_query_fetch_all",
        return_value=rows,
    ):
        dbs = workspaceClient.get_databases()
        assert next(dbs) == synapseDatabase


def test_get_dedicated_sql_pool_databases():
    rows = [
        [
            synapseDatabase.id,
            synapseDatabase.name,
            synapseDatabase.create_time,
            synapseDatabase.collation_name,
        ]
    ]
    with patch(
        "metaphor.synapse.workspace_client.WorkspaceClient._sql_query_fetch_all",
        return_value=rows,
    ):
        dbs = workspaceClient.get_databases(True)
        assert next(dbs) == synapseDatabase


def test_get_tables():
    col_dict = {}
    col_dict[synapseColumn1.name] = synapseColumn1
    col_dict[synapseColumn2.name] = synapseColumn2
    table = SynapseTable(
        id="mock_table_id",
        name="mock_table",
        schema_name="dbo",
        type="U",
        create_time=to_utc_time(datetime.now()),
        column_dict=col_dict,
        is_external=True,
        external_source="http://mock_data_source",
        external_file_format="PARQUET",
    )
    rows = []
    for column in table.column_dict.values():
        row = []
        row.append(table.id)
        row.append(table.name)
        row.append(table.schema_name)
        row.append(table.type)
        row.append(table.create_time)
        row.append(column.name)
        row.append(column.max_length)
        row.append(column.precision)
        row.append(column.is_nullable)
        row.append(column.type)
        row.append(column.is_unique)
        row.append(column.is_primary_key)
        row.append(table.is_external)
        row.append(column.is_foreign_key)
        rows.append(row)

    external_data = [table.external_source, table.external_file_format]

    with patch(
        "metaphor.synapse.workspace_client.WorkspaceClient._sql_query_fetch_all",
        side_effect=[rows, [external_data]],
    ):
        tables = workspaceClient.get_tables("mock_database")
        assert len(tables) == 1
        assert tables[0] == table


def test_get_dedicated_sql_pool_tables():
    col_dict = {}
    col_dict[synapseColumn1.name] = synapseColumn1
    col_dict[synapseColumn2.name] = synapseColumn2
    table = SynapseTable(
        id="mock_table_id",
        name="mock_table",
        schema_name="dbo",
        type="U",
        create_time=to_utc_time(datetime.now()),
        column_dict=col_dict,
        is_external=False,
    )
    rows = []
    for column in table.column_dict.values():
        row = []
        row.append(table.id)
        row.append(table.name)
        row.append(table.schema_name)
        row.append(table.type)
        row.append(table.create_time)
        row.append(column.name)
        row.append(column.max_length)
        row.append(column.precision)
        row.append(column.is_nullable)
        row.append(column.type)
        row.append(column.is_unique)
        row.append(column.is_primary_key)
        row.append(table.is_external)
        row.append(column.is_foreign_key)
        rows.append(row)

    with patch(
        "metaphor.synapse.workspace_client.WorkspaceClient._sql_query_fetch_all",
        return_value=rows,
    ):
        tables = workspaceClient.get_tables("mock_database", True)
        assert len(tables) == 1
        assert tables[0] == table


def test_get_sql_pool_query_log():
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
        querylogs = workspaceClient.get_sql_pool_query_logs(start_date, end_date)
        assert next(querylogs) == querylog1
        assert next(querylogs) == querylog2


def test_get_dedicated_sql_pool_query_logs():
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
            "database", start_date, end_date
        )
        assert next(querylogs) == querylog1
        assert next(querylogs) == querylog2
