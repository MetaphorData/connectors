import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    ColumnTypeName,
    SchemaInfo,
    TableInfo,
    TableType,
)

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.unity_catalog.config import UnityCatalogRunConfig
from metaphor.unity_catalog.profile.extractor import UnityCatalogProfileExtractor
from tests.test_utils import load_json


def dummy_config():
    return UnityCatalogRunConfig(
        hostname="dummy.host",
        http_path="path",
        token="",
        output=OutputConfig(),
    )


@patch("metaphor.unity_catalog.profile.extractor.create_connection")
@patch("metaphor.unity_catalog.profile.extractor.create_api")
@pytest.mark.asyncio
async def test_extractor(
    mock_create_api: MagicMock,
    mock_create_connection: MagicMock,
    test_root_dir: str,
):
    mock_client = MagicMock()

    mock_client.catalogs = MagicMock()
    mock_client.catalogs.list = MagicMock()
    mock_client.catalogs.list.return_value = [CatalogInfo(name="catalog")]
    mock_client.schemas = MagicMock()
    mock_client.schemas.list = MagicMock()
    mock_client.schemas.list.return_value = [
        SchemaInfo(name="schema"),
    ]
    mock_client.tables = MagicMock()
    mock_client.tables.list = MagicMock()
    mock_client.tables.list.return_value = [
        TableInfo(
            name="table",
            full_name="catalog.schema.table",
            table_type=TableType.MANAGED,
            columns=[
                ColumnInfo(name="col1", type_name=ColumnTypeName.STRING),
                ColumnInfo(name="col2", type_name=ColumnTypeName.INT),
            ],
        ),
    ]
    mock_create_api.return_value = mock_client

    mock_rows = [
        SimpleNamespace(col_name="Statistics", data_type="5566 bytes, 9487 rows")
    ]

    mock_col2_stats = [
        SimpleNamespace(info_name="distinct_count", info_value="1234"),
        SimpleNamespace(info_name="max", info_value="9999"),
        SimpleNamespace(info_name="min", info_value="-9999"),
        SimpleNamespace(info_name="num_nulls", info_value="NULL"),
    ]

    mock_latest_history = [
        {
            "timestamp": datetime.datetime.fromisoformat("2023-10-30T12:06:19+00:00"),
            "operation": "CREATE TABLE",
        }
    ]

    mock_cursor = MagicMock()
    mock_cursor.fetchall = MagicMock()
    mock_cursor.fetchall.side_effect = [
        mock_rows,
        mock_col2_stats,
        mock_latest_history,
    ]

    mock_cursor_ctx = MagicMock()
    mock_cursor_ctx.__enter__ = MagicMock()
    mock_cursor_ctx.__enter__.return_value = mock_cursor

    mock_connection = MagicMock()
    mock_connection.cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor_ctx

    mock_create_connection.return_value = mock_connection

    extractor = UnityCatalogProfileExtractor(dummy_config())
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/unity_catalog/profile/expected.json")
