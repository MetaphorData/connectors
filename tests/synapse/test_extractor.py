from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.synapse.config import SynapseConfig, SynapseQueryLogConfig
from metaphor.synapse.extractor import SynapseExtractor
from metaphor.synapse.model import (
    DedicatedSqlPoolSchema,
    DedicatedSqlPoolTable,
    SynapseTable,
    SynapseWorkspace,
    WorkspaceDatabase,
    QueryLogTable
)
from tests.test_utils import load_json
from datetime import datetime

mock_tenant_id = "mock_tenant_id"


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    mock_auth_instance = MagicMock()
    mock_workspace_instance = MagicMock()

    workspaceDatabase = WorkspaceDatabase(
        id="mock_database_id",
        name="mock_database_name",
        type="DATABASE",
        properties={"Origin": {"Type": "mock_database_type"}},
    )

    synapseTable = SynapseTable(
        id="mock_synapse_table_id",
        name="mock_synapse_table_name",
        type="TABLE",
        properties={
            "StorageDescriptor": {
                "Columns": [
                    {
                        "Name": "mock_column_name_1",
                        "Description": "mock_column_description_1",
                        "OriginDataTypeName": {
                            "Length": 0,
                            "IsNullable": False,
                            "Precision": 0,
                            "TypeName": "mock_column_type_1",
                        },
                    },
                    {
                        "Name": "mock_column_name_2",
                        "Description": "mock_column_description_2",
                        "OriginDataTypeName": {
                            "Length": 0,
                            "IsNullable": True,
                            "Precision": 0,
                            "TypeName": "mock_column_type_2",
                        },
                    },
                ],
                "Format": {
                    "FormatType": "CSV",
                },
                "Source": {"Location": "mock_storage_location"},
            },
            "Properties": {"PrimaryKeys": "primarykey1,primarykey2"},
            "TableType": "mock_table_type",
        },
    )

    synapseWorkspace = SynapseWorkspace(
        id="mock_synapse_workspace_id",
        name="mock_synapse_workspace_name",
        type="WORKSPACE",
    )

    mock_auth_instance._tenant_id = mock_tenant_id

    mock_auth_instance.get_list_workspace_clients = MagicMock(
        return_value=[mock_workspace_instance]
    )
    mock_workspace_instance._workspace = synapseWorkspace
    mock_workspace_instance.get_databases = MagicMock(return_value=[workspaceDatabase])
    mock_workspace_instance.get_tables = MagicMock(return_value=[synapseTable])

    with patch("metaphor.synapse.extractor.AuthClient") as mock_client:
        mock_client.return_value = mock_auth_instance

        config = SynapseConfig(
            output=OutputConfig(),
            tenant_id=mock_tenant_id,
            client_id="mock_client_id",
            secret="mock_secret",
            subscription_id="mock_subscription_id",
        )
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/synapse/expected.json")


@pytest.mark.asyncio
async def test_dedicated_sql_pool_extractor(test_root_dir):
    mock_auth_instance = MagicMock()
    mock_workspace_instance = MagicMock()

    workspaceSqlPoolDatabase = WorkspaceDatabase(
        id="mock_sql_pool_database_id",
        name="mock_sql_pool_database_name",
        type="Microsoft.Synapse/workspaces/sqlPools",
    )

    sqlpool_table = DedicatedSqlPoolTable(
        id="mock_sql_pool_table_id",
        name="mock_sql_pool_name",
        type="Microsoft.Synapse/workspaces/sqlPools/schemas/tables",
        sqlSchema=DedicatedSqlPoolSchema(
            id="mock_sql_pool_schema_id",
            name="dbo",
            type="Microsoft.Synapse/workspaces/sqlPools/schemas",
        ),
        columns=[
            {
                "id": "mock_sql_pool_schema_column_id_1",
                "name": "mock_sql_pool_schema_column_name_1",
                "type": "Microsoft.Synapse/workspaces/sqlPools/schemas/tables/columns",
                "properties": {"columnType": "int"},
            },
            {
                "id": "mock_sql_pool_schema_column_id_2",
                "name": "mock_sql_pool_schema_column_name_2",
                "type": "Microsoft.Synapse/workspaces/sqlPools/schemas/tables/columns",
                "properties": {"columnType": "nvarchar"},
            },
        ],
    )

    synapseWorkspace = SynapseWorkspace(
        id="mock_synapse_workspace_id",
        name="mock_synapse_workspace_name",
        type="WORKSPACE",
    )

    mock_auth_instance._tenant_id = mock_tenant_id

    mock_auth_instance.get_list_workspace_clients = MagicMock(
        return_value=[mock_workspace_instance]
    )

    mock_workspace_instance._workspace = synapseWorkspace

    mock_workspace_instance.get_dedicated_sql_pool_databases = MagicMock(
        return_value=[workspaceSqlPoolDatabase]
    )
    mock_workspace_instance.get_dedicated_sql_pool_tables = MagicMock(
        return_value=[sqlpool_table]
    )

    with patch("metaphor.synapse.extractor.AuthClient") as mock_client:
        mock_client.return_value = mock_auth_instance

        config = SynapseConfig(
            output=OutputConfig(),
            tenant_id=mock_tenant_id,
            client_id="mock_client_id",
            secret="mock_secret",
            subscription_id="mock_subscription_id",
        )
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/synapse/expected_sqlpool.json")


@pytest.mark.asyncio
async def test_extractor_with_query_log(test_root_dir):
    mock_auth_instance = MagicMock()
    mock_workspace_instance = MagicMock()

    workspaceDatabase = WorkspaceDatabase(
        id="mock_database_id",
        name="mock_database_name",
        type="DATABASE",
        properties={"Origin": {"Type": "mock_database_type"}},
    )

    synapseTable = SynapseTable(
        id="mock_synapse_table_id",
        name="mock_synapse_table_name",
        type="TABLE",
        properties={
            "StorageDescriptor": {
                "Columns": [
                    {
                        "Name": "mock_column_name_1",
                        "Description": "mock_column_description_1",
                        "OriginDataTypeName": {
                            "Length": 0,
                            "IsNullable": False,
                            "Precision": 0,
                            "TypeName": "mock_column_type_1",
                        },
                    },
                    {
                        "Name": "mock_column_name_2",
                        "Description": "mock_column_description_2",
                        "OriginDataTypeName": {
                            "Length": 0,
                            "IsNullable": True,
                            "Precision": 0,
                            "TypeName": "mock_column_type_2",
                        },
                    },
                ],
                "Format": {
                    "FormatType": "CSV",
                },
                "Source": {"Location": "mock_storage_location"},
            },
            "Properties": {"PrimaryKeys": "primarykey1,primarykey2"},
            "TableType": "mock_table_type",
        },
    )

    synapseWorkspace = SynapseWorkspace(
        id="mock_synapse_workspace_id",
        name="mock_synapse_workspace_name",
        type="WORKSPACE",
    )
    
    qyeryLogTable = QueryLogTable(
        request_id="mock_request_id",
        sql_query="SELECT TOP 10(*) FROM mock_query_table",
        login_name="mock_user@gmail.com",
        start_time=QueryLogTable.to_utc_time(datetime(2022, 11, 15, 13, 10, 10, 922321)),
        end_time=QueryLogTable.to_utc_time(datetime(2022, 11, 15, 13, 10, 11, 922321)),
        duration= 1000,
        query_size=10,
        error=None,
        query_operation="SELECT"
    )

    mock_auth_instance._tenant_id = mock_tenant_id

    mock_auth_instance.get_list_workspace_clients = MagicMock(
        return_value=[mock_workspace_instance]
    )
    mock_workspace_instance._workspace = synapseWorkspace
    mock_workspace_instance.get_databases = MagicMock(return_value=[workspaceDatabase])
    mock_workspace_instance.get_tables = MagicMock(return_value=[synapseTable])
    mock_workspace_instance.get_sql_pool_query_logs = MagicMock(return_value=[qyeryLogTable])

    with patch("metaphor.synapse.extractor.AuthClient") as mock_client:
        mock_client.return_value = mock_auth_instance

        config = SynapseConfig(
            output=OutputConfig(),
            tenant_id=mock_tenant_id,
            client_id="mock_client_id",
            secret="mock_secret",
            subscription_id="mock_subscription_id",
            query_log=SynapseQueryLogConfig(
                username="mock_username",
                password="mock_password",
                lookback_days=1
            )
        )
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/synapse/expected_with_query_log.json")
    

@pytest.mark.asyncio
async def test_dedicated_sql_pool_extractor_with_query_log(test_root_dir):
    mock_auth_instance = MagicMock()
    mock_workspace_instance = MagicMock()

    workspaceSqlPoolDatabase = WorkspaceDatabase(
        id="mock_sql_pool_database_id",
        name="mock_sql_pool_database_name",
        type="Microsoft.Synapse/workspaces/sqlPools",
    )

    sqlpool_table = DedicatedSqlPoolTable(
        id="mock_sql_pool_table_id",
        name="mock_sql_pool_name",
        type="Microsoft.Synapse/workspaces/sqlPools/schemas/tables",
        sqlSchema=DedicatedSqlPoolSchema(
            id="mock_sql_pool_schema_id",
            name="dbo",
            type="Microsoft.Synapse/workspaces/sqlPools/schemas",
        ),
        columns=[
            {
                "id": "mock_sql_pool_schema_column_id_1",
                "name": "mock_sql_pool_schema_column_name_1",
                "type": "Microsoft.Synapse/workspaces/sqlPools/schemas/tables/columns",
                "properties": {"columnType": "int"},
            },
            {
                "id": "mock_sql_pool_schema_column_id_2",
                "name": "mock_sql_pool_schema_column_name_2",
                "type": "Microsoft.Synapse/workspaces/sqlPools/schemas/tables/columns",
                "properties": {"columnType": "nvarchar"},
            },
        ],
    )

    synapseWorkspace = SynapseWorkspace(
        id="mock_synapse_workspace_id",
        name="mock_synapse_workspace_name",
        type="WORKSPACE",
    )
    
    qyeryLogTable = QueryLogTable(
        request_id="mock_request_id",
        session_id="mock_session_id",
        sql_query="INSERT INTO mock_query_table VALUE('test_id', 'test_name', 10)",
        login_name="mock_user@gmail.com",
        start_time=QueryLogTable.to_utc_time(datetime(2022, 11, 11, 13, 9, 11, 822321)),
        end_time=QueryLogTable.to_utc_time(datetime(2022, 11, 11, 13, 9, 11, 922321)),
        duration= 100,
        row_count=1,
        error=None,
        query_operation="INSERT"
    )

    mock_auth_instance._tenant_id = mock_tenant_id

    mock_auth_instance.get_list_workspace_clients = MagicMock(
        return_value=[mock_workspace_instance]
    )

    mock_workspace_instance._workspace = synapseWorkspace

    mock_workspace_instance.get_dedicated_sql_pool_databases = MagicMock(
        return_value=[workspaceSqlPoolDatabase]
    )
    mock_workspace_instance.get_dedicated_sql_pool_tables = MagicMock(
        return_value=[sqlpool_table]
    )
    
    mock_workspace_instance.get_sql_pool_query_logs = MagicMock(return_value=[qyeryLogTable])
    

    with patch("metaphor.synapse.extractor.AuthClient") as mock_client:
        mock_client.return_value = mock_auth_instance

        config = SynapseConfig(
            output=OutputConfig(),
            tenant_id=mock_tenant_id,
            client_id="mock_client_id",
            secret="mock_secret",
            subscription_id="mock_subscription_id",
            query_log=SynapseQueryLogConfig(
                username="mock_username",
                password="mock_password",
                lookback_days=10
            )
        )
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/synapse/expected_sqlpool_with_query_log.json")
