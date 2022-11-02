from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.synapse.config import SynapseConfig
from metaphor.synapse.extractor import SynapseExtractor
from metaphor.synapse.workspace_client import (
    DedicatedSqlPoolSchema,
    DedicatedSqlPoolTable,
    SynapseTable,
    WorkspaceDatabase,
)
from tests.test_utils import load_json


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

    mock_auth_instance.get_list_workspace_clients = MagicMock(
        return_value=[mock_workspace_instance]
    )
    mock_workspace_instance.get_databases = MagicMock(return_value=[workspaceDatabase])
    mock_workspace_instance.get_tables = MagicMock(return_value=[synapseTable])

    with patch("metaphor.synapse.extractor.AuthClient") as mock_client:
        mock_client.return_value = mock_auth_instance

        config = SynapseConfig(
            output=OutputConfig(),
            tenant_id="mock_tenat_id",
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

    mock_auth_instance.get_list_workspace_clients = MagicMock(
        return_value=[mock_workspace_instance]
    )
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
            tenant_id="mock_tenat_id",
            client_id="mock_client_id",
            secret="mock_secret",
            subscription_id="mock_subscription_id",
        )
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/synapse/expected_sqlpool.json")
