from datetime import datetime
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from google.cloud.bigquery import SchemaField

from metaphor.bigquery.lineage.config import BigQueryLineageRunConfig
from metaphor.bigquery.lineage.extractor import BigQueryLineageExtractor
from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from tests.bigquery.load_entries import load_entries
from tests.bigquery.test_extractor import (
    mock_dataset,
    mock_get_table,
    mock_list_datasets,
    mock_list_tables,
    mock_table,
    mock_table_full,
)
from tests.test_utils import load_json


def mock_list_entries(mock_build_log_client, entries):
    def side_effect(page_size, filter_):
        return entries

    mock_build_log_client.return_value.list_entries.side_effect = side_effect


@pytest.mark.asyncio
@freeze_time("2022-01-27")
async def test_log_extractor(test_root_dir):
    config = BigQueryLineageRunConfig(
        output=OutputConfig(), key_path="fake_file", enable_view_lineage=False
    )
    extractor = BigQueryLineageExtractor()

    entries = load_entries(test_root_dir + "/bigquery/lineage/data/sample_log.json")

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch(
        "metaphor.bigquery.lineage.extractor.build_logging_client"
    ) as mock_build_client:
        mock_build_client.return_value.project = "project1"
        mock_list_entries(mock_build_client, entries)

        events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(test_root_dir + "/bigquery/lineage/data/result.json")


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_view_extractor(test_root_dir):
    config = BigQueryLineageRunConfig(
        output=OutputConfig(),
        key_path="fake_file",
        project_id="fake_project",
        enable_lineage_from_log=False,
    )
    extractor = BigQueryLineageExtractor()

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch("metaphor.bigquery.lineage.extractor.build_client") as mock_build_client:
        mock_build_client.return_value.project = "project1"

        mock_list_datasets(mock_build_client, [mock_dataset("dataset1")])

        mock_list_tables(
            mock_build_client,
            {
                "dataset1": [
                    mock_table("dataset1", "table1"),
                ],
            },
        )

        mock_get_table(
            mock_build_client,
            {
                ("dataset1", "table1"): mock_table_full(
                    dataset_id="dataset1",
                    table_id="table1",
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

    assert events == load_json(
        f"{test_root_dir}/bigquery/lineage/data/view_result.json"
    )
