import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo, TableType

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.unity_catalog.profile.config import UnityCatalogProfileRunConfig
from metaphor.unity_catalog.profile.extractor import UnityCatalogProfileExtractor
from tests.test_utils import load_json


def dummy_config():
    return UnityCatalogProfileRunConfig(
        host="http://dummy.host",
        token="",
        output=OutputConfig(),
        warehouse_id=None,
    )


@patch(
    "metaphor.unity_catalog.profile.extractor.UnityCatalogProfileExtractor.create_connection"
)
@patch("metaphor.unity_catalog.extractor.UnityCatalogExtractor.create_api")
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
            name="table", full_name="catalog.schema.table", table_type=TableType.MANAGED
        ),
    ]
    mock_create_api.return_value = mock_client

    mock_rows = [
        SimpleNamespace(col_name="Statistics", data_type="5566 bytes, 9487 rows")
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


def test_bad_warehouse():
    client = MagicMock()
    client.warehouses = MagicMock()
    client.warehouses.list = MagicMock()
    client.warehouses.list.return_value = iter([])

    with pytest.raises(ValueError):
        UnityCatalogProfileExtractor.create_connection(client, "token", None)

    client.warehouses.list.return_value = iter(["530e470a55aeb40d"])
    client.warehouses.get = MagicMock(
        side_effect=DatabricksError("SQL warehouse 530e470a55aeb40e does not exist.")
    )

    with pytest.raises(ValueError):
        UnityCatalogProfileExtractor.create_connection(
            client, "token", "530e470a55aeb40e"
        )
