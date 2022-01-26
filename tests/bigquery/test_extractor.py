from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time
from google.cloud.bigquery.schema import SchemaField

from metaphor.bigquery.extractor import BigQueryExtractor, BigQueryRunConfig
from metaphor.common.event_util import EventUtil
from tests.test_utils import load_json


def mock_dataset(dataset_id):
    dataset = MagicMock()
    dataset.dataset_id = dataset_id
    return dataset


def mock_table(dataset_id, table_id):
    table = MagicMock()
    table.dataset_id = dataset_id
    table.table_id = table_id
    return table


def mock_table_full(
    dataset_id,
    table_id,
    table_type,
    description,
    schema=[],
    view_query="",
    mview_query="",
    num_bytes=0,
    num_rows=0,
    modified=datetime.fromisoformat("2000-01-01"),
):
    table = MagicMock()
    table.dataset_id = dataset_id
    table.table_id = table_id
    table.table_type = table_type
    table.description = description
    table.schema = schema
    table.view_query = view_query
    table.mview_query = mview_query
    table.num_bytes = num_bytes
    table.num_rows = num_rows
    table.modified = modified.replace(tzinfo=timezone.utc)
    return table


def mock_field(name, description, field_type, is_nullable):
    field = MagicMock()
    field.name = name
    field.description = description
    field.field_type = field_type
    field.is_nullable = is_nullable
    return field


def mock_list_datasets(mock_build_client, datasets):
    mock_build_client.return_value.list_datasets.return_value = datasets


def mock_list_tables(mock_build_client, tables):
    def side_effect(dataset_id):
        return tables.get(dataset_id, [])

    mock_build_client.return_value.list_tables.side_effect = side_effect


def mock_get_table(mock_build_client, tables):
    def side_effect(table_ref):
        return tables.get((table_ref.dataset_id, table_ref.table_id), None)

    mock_build_client.return_value.get_table.side_effect = side_effect


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):
    config = BigQueryRunConfig(
        output=None, key_path="fake_file", project_id="fake_project"
    )
    extractor = BigQueryExtractor()

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch("metaphor.bigquery.extractor.build_client") as mock_build_client:
        mock_build_client.return_value.project = "project1"

        mock_list_datasets(mock_build_client, [mock_dataset("dataset1")])

        mock_list_tables(
            mock_build_client,
            {
                "dataset1": [
                    mock_table("dataset1", "table1"),
                    mock_table("dataset1", "table2"),
                ],
            },
        )

        mock_get_table(
            mock_build_client,
            {
                ("dataset1", "table1"): mock_table_full(
                    dataset_id="dataset1",
                    table_id="table1",
                    table_type="TABLE",
                    description="description",
                    schema=[
                        SchemaField(
                            name="f1",
                            field_type="STRING",
                            description="d1",
                            mode="NULLABLE",
                        ),
                        SchemaField(
                            name="f2",
                            field_type="INTEGER",
                            description="d2",
                            mode="REQUIRED",
                        ),
                    ],
                    modified=datetime.fromisoformat("2000-01-02"),
                    num_bytes=5 * 1024 * 1024,
                    num_rows=100,
                ),
                ("dataset1", "table2"): mock_table_full(
                    dataset_id="dataset1",
                    table_id="table2",
                    table_type="VIEW",
                    description="description",
                    schema=[
                        SchemaField(
                            name="f1",
                            field_type="FLOAT",
                            description="d1",
                            mode="REPEATED",
                        )
                    ],
                    view_query="select * from FOO",
                    modified=datetime.fromisoformat("2000-01-02"),
                    num_bytes=512 * 1024,
                    num_rows=1000,
                ),
            },
        )

        events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(f"{test_root_dir}/bigquery/expected.json")
