from unittest.mock import MagicMock, patch

import pytest

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


@patch("metaphor.bigquery.lineage.extractor.build_client")
@patch("metaphor.bigquery.lineage.extractor.build_logging_client")
@patch("metaphor.bigquery.lineage.extractor.get_credentials")
@pytest.mark.asyncio
async def test_log_extractor(
    mock_get_credentials: MagicMock,
    mock_build_logging_client: MagicMock,
    mock_build_client: MagicMock,
    test_root_dir: str,
):
    config = BigQueryLineageRunConfig(
        project_id="pid",
        output=OutputConfig(),
        key_path="fake_file",
        enable_view_lineage=False,
        include_self_lineage=True,
    )

    entries = load_entries(test_root_dir + "/bigquery/lineage/data/sample_log.json")

    extractor = BigQueryLineageExtractor(config)

    mock_get_credentials.return_value = "fake_credentials"

    mock_build_logging_client.return_value.project = "project1"

    mock_list_entries(mock_build_logging_client, entries)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(test_root_dir + "/bigquery/lineage/data/result.json")


@patch("metaphor.bigquery.lineage.extractor.build_client")
@patch("metaphor.bigquery.lineage.extractor.build_logging_client")
@patch("metaphor.bigquery.lineage.extractor.get_credentials")
@pytest.mark.asyncio
async def test_view_extractor(
    mock_get_credentials: MagicMock,
    mock_build_logging_client: MagicMock,
    mock_build_client: MagicMock,
    test_root_dir: str,
):
    config = BigQueryLineageRunConfig(
        output=OutputConfig(),
        key_path="fake_file",
        project_id="fake_project",
        enable_lineage_from_log=False,
    )

    extractor = BigQueryLineageExtractor(config)

    mock_get_credentials.return_value = "fake_credentials"

    mock_build_client.return_value.project = "project1"

    mock_list_datasets(mock_build_client, [mock_dataset("dataset1")])

    mock_list_tables(
        mock_build_client,
        {
            "dataset1": [
                mock_table("dataset1", "table1"),
                mock_table("dataset1", "table2"),
                mock_table("dataset1", "table3"),
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
                view_query="select * from `foo`",
            ),
            ("dataset1", "table2"): mock_table_full(
                dataset_id="dataset1",
                table_id="table2",
                table_type="VIEW",
                description="description",
                view_query="select * from `Foo`",
            ),
            ("dataset1", "table3"): mock_table_full(
                dataset_id="dataset1",
                table_id="table3",
                table_type="VIEW",
                description="description",
                view_query="select * from foo",
            ),
        },
    )

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(
        f"{test_root_dir}/bigquery/lineage/data/view_result.json"
    )
