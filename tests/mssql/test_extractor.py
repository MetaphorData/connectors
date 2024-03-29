from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.filter import DatasetFilter
from metaphor.common.utils import to_utc_time
from metaphor.mssql.config import MssqlConfig
from metaphor.mssql.extractor import MssqlExtractor
from metaphor.mssql.model import (
    MssqlColumn,
    MssqlConnectConfig,
    MssqlDatabase,
    MssqlForeignKey,
    MssqlTable,
)
from tests.test_utils import load_json

mock_tenant_id = "mock_tenant_id"

mssql_config = MssqlConfig(
    output=OutputConfig(),
    tenant_id=mock_tenant_id,
    server_name="mock_server_name",
    endpoint="mock_server_name.database.windoes.net",
    username="username",
    password="password",
)

mssql_connect_config = MssqlConnectConfig(
    endpoint=f"{mssql_config.server_name}.database.windoes.net",
    username=mssql_config.username,
    password=mssql_config.password,
)


@patch("metaphor.mssql.extractor.MssqlClient")
@pytest.mark.asyncio
async def test_extractor(mock_client: MagicMock, test_root_dir: str):
    mssqlDatabase = MssqlDatabase(
        id=1,
        name="mock_database_name",
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    column_map = {}
    column_map["mock_column_name_1"] = MssqlColumn(
        name="mock_column_name_1",
        type="mock_column_type_1",
        max_length=0.0,
        precision=0.0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=True,
    )
    column_map["mock_column_name_2"] = MssqlColumn(
        name="mock_column_name_2",
        type="mock_column_type_2",
        max_length=0.0,
        precision=0.0,
        is_nullable=True,
        is_unique=False,
        is_primary_key=False,
    )

    mssqlTable = MssqlTable(
        id="mock_mssql_table_id",
        name="mock_mssql_table_name",
        schema_name="mock_mssql_table_schema",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        is_external=True,
        external_file_format="CSV",
        external_source="mock_storage_location",
    )

    mock_client_instance = MagicMock()
    mock_client_instance.config = mssql_connect_config
    mock_client_instance.get_databases = MagicMock(return_value=[mssqlDatabase])
    mock_client_instance.get_tables = MagicMock(return_value=[mssqlTable])

    mock_client.return_value = mock_client_instance

    config = mssql_config
    extractor = MssqlExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/mssql/expected.json")


@patch("metaphor.mssql.extractor.MssqlClient")
@pytest.mark.asyncio
async def test_extractor_with_foreign_keys(mock_client: MagicMock, test_root_dir: str):
    mssqlDatabase = MssqlDatabase(
        id=1,
        name="mock_database_name",
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    column_map = {}
    column_map["mock_column_name_1"] = MssqlColumn(
        name="mock_column_name_1",
        type="mock_column_type_1",
        max_length=0.0,
        precision=0.0,
        is_nullable=False,
        is_unique=False,
        is_primary_key=True,
    )
    column_map["mock_column_name_2"] = MssqlColumn(
        name="mock_column_name_2",
        type="mock_column_type_2",
        max_length=0.0,
        precision=0.0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=False,
    )

    mssqlTable = MssqlTable(
        id="mock_mssql_table_id",
        name="mock_mssql_table_name",
        schema_name="mock_mssql_table_schema",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 30, 11, 30, 15, 822321)),
        is_external=False,
    )

    column_map = {}
    column_map["mock_column2_name_1"] = MssqlColumn(
        name="mock_column2_name_1",
        type="mock_column2_type_1",
        max_length=0.0,
        precision=0.0,
        is_nullable=False,
        is_unique=False,
        is_primary_key=True,
    )
    column_map["mock_column2_name_2"] = MssqlColumn(
        name="mock_column2_name_2",
        type="mock_column2_type_2",
        max_length=0.0,
        precision=0.0,
        is_nullable=False,
        is_unique=False,
        is_primary_key=False,
    )

    mssqlTable2 = MssqlTable(
        id="mock_mssql_table_id_2",
        name="mock_mssql_table_name_2",
        schema_name="mock_mssql_table_schema_2",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 30, 12, 30, 15, 822321)),
        is_external=False,
    )

    foreign_key = MssqlForeignKey(
        name="mock_table1_table2_foreign_key",
        table_id=mssqlTable2.id,
        column_name=mssqlTable2.column_dict["mock_column2_name_2"].name,
        referenced_table_id=mssqlTable.id,
        referenced_column=mssqlTable.column_dict["mock_column_name_2"].name,
    )

    mock_client_instance = MagicMock()
    mock_client_instance.config = mssql_connect_config
    mock_client_instance.get_databases = MagicMock(return_value=[mssqlDatabase])
    mock_client_instance.get_tables = MagicMock(return_value=[mssqlTable, mssqlTable2])
    mock_client_instance.get_foreign_keys = MagicMock(return_value=[foreign_key])

    mock_client.return_value = mock_client_instance

    config = mssql_config
    extractor = MssqlExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert len(events) == 2
    assert events == load_json(f"{test_root_dir}/mssql/expected_with_foreign_keys.json")


@patch("metaphor.mssql.extractor.MssqlClient")
@pytest.mark.asyncio
async def test_extractor_no_table(mock_client: MagicMock):
    mssqlDatabase = MssqlDatabase(
        id=1,
        name="mock_database_name",
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    mock_client_instance = MagicMock()
    mock_client_instance.config = mssql_connect_config
    mock_client_instance.get_databases = MagicMock(return_value=[mssqlDatabase])
    mock_client_instance.get_tables = MagicMock(return_value=[])

    mock_client.return_value = mock_client_instance

    config = mssql_config
    extractor = MssqlExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert len(events) == 0


@patch("metaphor.mssql.extractor.MssqlClient")
@pytest.mark.asyncio
async def test_extractor_with_filter(mock_client: MagicMock, test_root_dir: str):
    mssqlDatabase = MssqlDatabase(
        id=1,
        name="mock_database_name",
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    column_map = {}
    column_map["mock_column_name_1"] = MssqlColumn(
        name="mock_column_name_1",
        type="mock_column_type_1",
        max_length=0.0,
        precision=0.0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=True,
    )
    column_map["mock_column_name_2"] = MssqlColumn(
        name="mock_column_name_2",
        type="mock_column_type_2",
        max_length=0.0,
        precision=0.0,
        is_nullable=True,
        is_unique=False,
        is_primary_key=False,
    )

    mssqlTable = MssqlTable(
        id="mock_mssql_table_id",
        name="mock_mssql_table_name",
        schema_name="mock_mssql_table_schema",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        is_external=True,
        external_file_format="CSV",
        external_source="mock_storage_location",
    )

    column_map_2 = {}
    column_map_2["mock_column_2_name_1"] = MssqlColumn(
        name="mock_column_2_name_1",
        type="mock_column_2_type_1",
        max_length=0.0,
        precision=0.0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=True,
    )
    column_map_2["mock_column_2_name_2"] = MssqlColumn(
        name="mock_column_2_name_2",
        type="mock_column_2_type_2",
        max_length=0.0,
        precision=0.0,
        is_nullable=True,
        is_unique=False,
        is_primary_key=False,
    )

    mssqlTable2 = MssqlTable(
        id="mock_mssql_table_id_2",
        name="mock_mssql_table_name_2",
        schema_name="mock_mssql_table_schema",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2023, 1, 18, 17, 30, 15, 822321)),
        is_external=False,
    )

    mssqlDatabase2 = MssqlDatabase(
        id=2,
        name="mock_database_name_2",
        create_time=to_utc_time(datetime(2023, 1, 19, 17, 30, 15, 822321)),
        collation_name="Latin1_General_100_CI_AS_SC_UTF8",
    )

    column_map_3 = {}
    column_map_3["mock_column_name_3"] = MssqlColumn(
        name="mock_column_name_3",
        type="mock_column_type_3",
        max_length=0.0,
        precision=0.0,
        is_nullable=False,
        is_unique=True,
        is_primary_key=True,
    )
    column_map_3["mock_column_name_3"] = MssqlColumn(
        name="mock_column_name_3",
        type="mock_column_type_3",
        max_length=0.0,
        precision=0.0,
        is_nullable=True,
        is_unique=False,
        is_primary_key=False,
    )

    mssqlTable3 = MssqlTable(
        id="mock_mssql_table_id_3",
        name="mock_mssql_table_name_3",
        schema_name="mock_mssql_table_schema_3",
        type="TABLE",
        column_dict=column_map,
        create_time=to_utc_time(datetime(2022, 12, 9, 18, 30, 15, 822321)),
        is_external=False,
    )

    mock_client_instance = MagicMock()
    mock_client_instance.config = mssql_connect_config
    mock_client_instance.get_databases = MagicMock(
        return_value=[mssqlDatabase, mssqlDatabase2]
    )
    mock_client_instance.get_tables = MagicMock(
        side_effect=[[mssqlTable, mssqlTable2], [mssqlTable3]]
    )

    mock_client.return_value = mock_client_instance

    config = mssql_config
    datasetFilter = DatasetFilter()
    datasetFilter.includes = {
        "mock_database_name": {
            "mock_mssql_table_schema": set(["mock_mssql_table_name"]),
        },
        "mock_database_name_2": None,
    }
    datasetFilter.excludes = {
        "mock_database_name": {
            "mock_mssql_table_schema": set(["mock_mssql_table_name_2"])
        }
    }
    config.filter = datasetFilter
    extractor = MssqlExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/mssql/expected_with_filter.json")
