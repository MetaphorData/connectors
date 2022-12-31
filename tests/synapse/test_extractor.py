from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.utils import to_utc_time
from metaphor.mssql.model import (
    MssqlColumn,
    MssqlConnectConfig,
    MssqlDatabase,
    MssqlTable,
)
from metaphor.synapse.config import SynapseConfig, SynapseQueryLogConfig
from metaphor.synapse.extractor import SynapseExtractor
from metaphor.synapse.model import SynapseQueryLog
from tests.test_utils import load_json

mock_tenant_id = "mock_tenant_id"

synapse_config = SynapseConfig(
    output=OutputConfig(),
    tenant_id=mock_tenant_id,
    server_name="mock_synapse_workspace_name",
    username="username",
    password="password",
)


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    synapseDatabase = MssqlDatabase(
        id=1,
        name="mock_database_name",
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    column_map = {}
    column_map["mock_column_name_1"] = MssqlColumn(
        name="mock_column_name_1",
        type="mock_column_type_1",
        max_length=0,
        precision=0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=True,
    )
    column_map["mock_column_name_2"] = MssqlColumn(
        name="mock_column_name_2",
        type="mock_column_type_2",
        max_length=0,
        precision=0,
        is_nullable=True,
        is_unique=False,
        is_primary_key=False,
    )

    synapseTable = MssqlTable(
        id="mock_synapse_table_id",
        name="mock_synapse_table_name",
        schema_name="mock_synapse_table_schema",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        is_external=True,
        external_file_format="CSV",
        external_source="mock_storage_location",
    )

    mock_client_instance = MagicMock()
    mock_client_instance.get_databases = MagicMock(side_effect=[[synapseDatabase], []])
    mock_client_instance.get_tables = MagicMock(return_value=[synapseTable])

    with patch("metaphor.synapse.extractor.MssqlClient") as mock_client:
        mock_client.return_value = mock_client_instance

        config = synapse_config
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/synapse/expected.json")


@pytest.mark.asyncio
async def test_dedicated_sql_pool_extractor(test_root_dir):
    synapseDatabase = MssqlDatabase(
        id=1,
        name="mock_dedicated_database_name",
        create_time=to_utc_time(datetime(2022, 12, 14, 12, 30, 15, 622321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    column_map = {}
    column_map["mock_dedicated_column_name_1"] = MssqlColumn(
        name="mock_dedicated_column_name_1",
        type="mock_dedicated_column_type_1",
        max_length=0,
        precision=0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=True,
    )
    column_map["mock_dedicated_column_name_2"] = MssqlColumn(
        name="mock_dedicated_column_name_2",
        type="mock_dedicated_column_type_2",
        max_length=0,
        precision=0,
        is_nullable=True,
        is_unique=False,
        is_primary_key=False,
    )

    synapseTable = MssqlTable(
        id="mock_dedicated_table_id",
        name="mock_dedicated_table_name",
        schema_name="mock_dedicated_table_schema",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 14, 12, 30, 15, 622321)),
        is_external=False,
    )

    mock_client_instance = MagicMock()
    mock_client_instance.get_databases = MagicMock(side_effect=[[], [synapseDatabase]])
    mock_client_instance.get_tables = MagicMock(return_value=[synapseTable])

    with patch("metaphor.synapse.extractor.MssqlClient") as mock_client:
        mock_client.return_value = mock_client_instance

        config = synapse_config
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/synapse/expected_sqlpool.json")


@pytest.mark.asyncio
async def test_extractor_with_query_log(test_root_dir):
    synapseDatabase = MssqlDatabase(
        id=1,
        name="mock_database_name",
        type="DATABASE",
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    column_map = {}
    column_map["mock_column_name_1"] = MssqlColumn(
        name="mock_column_name_1",
        type="mock_column_type_1",
        max_length=0,
        precision=0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=True,
    )
    column_map["mock_column_name_2"] = MssqlColumn(
        name="mock_column_name_2",
        type="mock_column_type_2",
        max_length=0,
        precision=0,
        is_nullable=True,
        is_unique=False,
        is_primary_key=False,
    )

    synapseTable = MssqlTable(
        id="mock_synapse_table_id",
        name="mock_synapse_table_name",
        schema_name="mock_synapse_table_schema",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        is_external=True,
        external_file_format="CSV",
        external_source="mock_storage_location",
    )

    qyeryLogTable = SynapseQueryLog(
        request_id="mock_request_id",
        sql_query="SELECT TOP 10(*) FROM mock_query_table",
        login_name="mock_user@gmail.com",
        start_time=to_utc_time(datetime(2022, 11, 15, 13, 10, 10, 922321)),
        end_time=to_utc_time(datetime(2022, 11, 15, 13, 10, 11, 922321)),
        duration=1000,
        query_size=10,
        error=None,
        query_operation="SELECT",
    )

    mock_client_instance = MagicMock()
    mock_client_instance.get_databases = MagicMock(side_effect=[[synapseDatabase], []])
    mock_client_instance.get_tables = MagicMock(return_value=[synapseTable])

    with patch("metaphor.synapse.extractor.MssqlClient") as mock_client, patch(
        "metaphor.synapse.extractor.WorkspaceQuery.get_sql_pool_query_logs"
    ) as mock_query:
        mock_client.return_value = mock_client_instance
        mock_query.return_value = [qyeryLogTable]

        config = synapse_config
        config.query_log = SynapseQueryLogConfig(lookback_days=1)
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/synapse/expected_with_query_log.json")


@pytest.mark.asyncio
async def test_dedicated_sql_pool_extractor_with_query_log(test_root_dir):
    synapseDatabase = MssqlDatabase(
        id=1,
        name="mock_dedicated_database_name",
        create_time=to_utc_time(datetime(2022, 12, 14, 12, 30, 15, 622321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    column_map = {}
    column_map["mock_dedicated_column_name_1"] = MssqlColumn(
        name="mock_dedicated_column_name_1",
        type="mock_dedicated_column_type_1",
        max_length=0,
        precision=0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=True,
    )
    column_map["mock_dedicated_column_name_2"] = MssqlColumn(
        name="mock_dedicated_column_name_2",
        type="mock_dedicated_column_type_2",
        max_length=0,
        precision=0,
        is_nullable=True,
        is_unique=False,
        is_primary_key=False,
    )

    synapseTable = MssqlTable(
        id="mock_dedicated_table_id",
        name="mock_dedicated_table_name",
        schema_name="mock_dedicated_table_schema",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 14, 12, 30, 15, 622321)),
        is_external=False,
    )

    qyeryLogTable = SynapseQueryLog(
        request_id="mock_request_id",
        session_id="mock_session_id",
        sql_query="INSERT INTO mock_query_table VALUE('test_id', 'test_name', 10)",
        login_name="mock_user@gmail.com",
        start_time=to_utc_time(datetime(2022, 12, 14, 12, 30, 11, 822321)),
        end_time=to_utc_time(datetime(2022, 12, 20, 12, 30, 11, 922321)),
        duration=100,
        row_count=1,
        error=None,
        query_operation="INSERT",
    )

    mock_client_instance = MagicMock()
    mock_client_instance.config = MssqlConnectConfig(
        endpoint="sdfs", username="username", password="password"
    )
    mock_client_instance.get_databases = MagicMock(side_effect=[[], [synapseDatabase]])
    mock_client_instance.get_tables = MagicMock(return_value=[synapseTable])

    with patch("metaphor.synapse.extractor.MssqlClient") as mock_client, patch(
        "metaphor.synapse.extractor.WorkspaceQuery.get_dedicated_sql_pool_query_logs"
    ) as mock_query:
        mock_client.return_value = mock_client_instance
        mock_query.return_value = [qyeryLogTable]

        config = synapse_config
        config.query_log = SynapseQueryLogConfig(lookback_days=1)
        extractor = SynapseExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    # import json
    # print(json.dumps(events, indent=2))
    assert events == load_json(
        f"{test_root_dir}/synapse/expected_sqlpool_with_query_log.json"
    )
