from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.oracle.extractor import OracleExtractor, OracleRunConfig
from tests.test_utils import load_json


def mock_inspector():
    mock_instance = MagicMock()

    mock_instance.get_schema_names = MagicMock(return_value=["dev", "prod"])
    mock_instance.get_table_names = MagicMock(
        side_effect=[["table1"], ["table2", "table3"]]
    )
    mock_instance.get_table_comment = MagicMock(
        side_effect=[{"text": "comment"}, {}, {}, {}, {}]
    )
    mock_instance.get_columns = MagicMock(
        side_effect=[
            [
                {
                    "name": "col1",
                    "comment": "col desc",
                    "type": "INTEGER",
                    "nullable": True,
                },
                {
                    "name": "col2",
                    "comment": None,
                    "type": "INTEGER",
                    "nullable": True,
                },
            ],
            [
                {
                    "name": "x",
                    "comment": "col desc",
                    "type": "VARCHAR(255)",
                    "nullable": True,
                },
                {
                    "name": "y",
                    "comment": None,
                    "type": "INTEGER",
                    "nullable": True,
                },
            ],
            [
                {
                    "name": "id",
                    "comment": "id",
                    "type": "VARCHAR(255)",
                    "nullable": True,
                },
                {
                    "name": "email",
                    "comment": None,
                    "type": "VARCHAR(255)",
                    "nullable": True,
                },
            ],
            [],
            [],
        ]
    )
    mock_instance.get_pk_constraint = MagicMock(
        side_effect=[
            {
                "constrained_columns": ["col1"],
            },
            {
                "constrained_columns": ["x"],
            },
            {
                "constrained_columns": ["id"],
            },
            {},
            {},
        ]
    )

    mock_instance.get_foreign_keys = MagicMock(
        side_effect=[
            [],
            [
                {
                    "referred_schema": "dev",
                    "referred_table": "table1",
                    "constrained_columns": ["x"],
                    "referred_columns": ["col1"],
                }
            ],
            [],
            [],
            [],
        ]
    )

    return mock_instance


def mock_connection():
    connection = MagicMock()

    cursor = MagicMock()

    connection.execute.return_value = cursor

    cursor.fetchall = MagicMock(
        side_effect=[
            [("SYS",)],  # get_system_users
            [],  # extract_views_names, dev
            [],  # extract_mviews_names, dev
            [("view",)],  # extract_views_names, prod
            [("mview",)],  # extract_mviews_names, prod
            [("TABLE1", "DEV", "DDL ......")],  # extract_ddl
            [("SYS",)],  # get_system_users
        ]
    )

    cursor.__iter__.return_value = iter(
        [
            (
                "DEV",
                "SELECT...",
                datetime.fromtimestamp(1722353493, tz=timezone.utc),
                10.0,
                "sql-id",
            )
        ]
    )

    return connection


def mock_connect(mocked_inspector: MagicMock, mocked_connection: MagicMock):
    mocked_inspector.engine.connect.return_value.__enter__.return_value = (
        mocked_connection
    )


@patch("metaphor.oracle.extractor.OracleExtractor.get_inspector")
@pytest.mark.asyncio
async def test_extractor(mock_get_inspector: MagicMock, test_root_dir: str):
    config = OracleRunConfig(
        output=OutputConfig(),
        user="user",
        password="password",
        host="127.0.0.1",
        alternative_host="foo.bar.com",
        database="db",
        port=1234,
    )

    mocked_inspector = mock_inspector()
    mock_get_inspector.return_value = mocked_inspector
    mocked_connection = mock_connection()
    mock_connect(mocked_inspector, mocked_connection)

    extractor = OracleExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/oracle/expected.json")

    query_logs = [EventUtil.trim_event(e) for e in extractor.collect_query_logs()]

    assert query_logs == load_json(f"{test_root_dir}/oracle/expected_query_logs.json")
