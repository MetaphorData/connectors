from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.database.extractor import (
    GenericDatabaseExtractor,
    GenericDatabaseRunConfig,
)
from tests.test_utils import load_json


def mock_inspector():
    mock_instance = MagicMock()

    mock_instance.get_schema_names = MagicMock(return_value=["schema1", "schema2"])
    mock_instance.get_table_names = MagicMock(
        side_effect=[["table1"], ["table2", "table3"]]
    )
    mock_instance.get_table_comment = MagicMock(
        side_effect=[{"text": "comment"}, {}, {}]
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
        ]
    )

    mock_instance.get_foreign_keys = MagicMock(
        side_effect=[
            [],
            [
                {
                    "referred_schema": "schema1",
                    "referred_table": "table1",
                    "constrained_columns": ["x"],
                    "referred_columns": ["col1"],
                }
            ],
            [],
        ]
    )

    return mock_instance


@patch("metaphor.database.extractor.GenericDatabaseExtractor.get_inspector")
@pytest.mark.asyncio
async def test_extractor(mock_get_inspector: MagicMock, test_root_dir: str):
    config = GenericDatabaseRunConfig(
        output=OutputConfig(),
        user="user",
        password="password",
        host="foo.bar.com",
        database="db",
        port=1234,
    )

    mock_get_inspector.return_value = mock_inspector()

    extractor = GenericDatabaseExtractor(config)
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/database/expected.json")
