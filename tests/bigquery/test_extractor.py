from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.schema import SchemaField

from metaphor.bigquery.config import BigQueryLineageConfig, BigQueryQueryLogConfig
from metaphor.bigquery.extractor import BigQueryExtractor, BigQueryRunConfig
from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from tests.bigquery.load_entries import load_entries
from tests.test_utils import load_json, wrap_query_log_stream_to_event


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
    created=datetime.fromisoformat("2000-01-01"),
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
    table.created = created.replace(tzinfo=timezone.utc)
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


def mock_list_entries(mock_build_log_client, entries):
    def side_effect(page_size, filter_):
        return entries

    mock_build_log_client.return_value.list_entries.side_effect = side_effect


@patch("metaphor.bigquery.extractor.build_client")
@patch("metaphor.bigquery.extractor.build_logging_client")
@patch("metaphor.bigquery.extractor.get_credentials")
@pytest.mark.asyncio
async def test_extractor(
    mock_get_credentials: MagicMock,
    mock_build_logging_client: MagicMock,
    mock_build_client: MagicMock,
    test_root_dir,
):
    config = BigQueryRunConfig(
        output=OutputConfig(),
        key_path="fake_file",
        project_ids=["fake_project"],
        query_log=BigQueryQueryLogConfig(),
        lineage=BigQueryLineageConfig(
            enable_lineage_from_log=False,
            enable_view_lineage=False,
        ),
    )

    entries = load_entries(test_root_dir + "/bigquery/sample_log.json")

    extractor = BigQueryExtractor(config)

    mock_get_credentials.return_value = "fake_credential"

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
                created=datetime.fromisoformat("2000-01-01"),
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
                created=datetime.fromisoformat("2000-01-01"),
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
                    ),
                    SchemaField(
                        name="f2",
                        field_type="RECORD",
                        description="d2",
                        mode="REQUIRED",
                        fields=[
                            SchemaField(
                                name="sf1",
                                field_type="INT",
                                description="d3",
                                mode="NULLABLE",
                            ),
                            SchemaField(
                                name="sf2",
                                field_type="STRING",
                                description="d4",
                                mode="REQUIRED",
                            ),
                        ],
                    ),
                ],
                view_query="select * from FOO",
                modified=datetime.fromisoformat("2000-01-02"),
                num_bytes=512 * 1024,
                num_rows=1000,
            ),
        },
    )

    mock_list_entries(mock_build_logging_client, entries)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/bigquery/expected.json")

    query_logs = wrap_query_log_stream_to_event(extractor.collect_query_logs())
    expected_query_logs = f"{test_root_dir}/bigquery/query_logs.json"
    assert query_logs == load_json(expected_query_logs)


@patch("metaphor.bigquery.extractor.build_client")
@patch("metaphor.bigquery.extractor.build_logging_client")
@patch("metaphor.bigquery.extractor.get_credentials")
@pytest.mark.asyncio
async def test_extract_view_upstream(
    mock_get_credentials: MagicMock,
    mock_build_logging_client: MagicMock,
    mock_build_client: MagicMock,
    test_root_dir: str,
) -> None:
    config = BigQueryRunConfig(
        project_ids=["project1"],
        output=OutputConfig(),
        key_path="fake_file",
        lineage=BigQueryLineageConfig(
            enable_lineage_from_log=False,
        ),
    )
    extractor = BigQueryExtractor(config)

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

    assert events == load_json(f"{test_root_dir}/bigquery/data/view_result.json")


@patch("metaphor.bigquery.extractor.build_client")
@patch("metaphor.bigquery.extractor.build_logging_client")
@patch("metaphor.bigquery.extractor.get_credentials")
@pytest.mark.asyncio
async def test_log_extractor(
    mock_get_credentials: MagicMock,
    mock_build_logging_client: MagicMock,
    mock_build_client: MagicMock,
    test_root_dir: str,
):
    config = BigQueryRunConfig(
        project_ids=["project1"],
        output=OutputConfig(),
        key_path="fake_file",
        lineage=BigQueryLineageConfig(
            enable_view_lineage=False,
            include_self_lineage=True,
        ),
    )

    entries = load_entries(test_root_dir + "/bigquery/data/sample_log.json")

    extractor = BigQueryExtractor(config)

    mock_get_credentials.return_value = "fake_credentials"

    mock_build_logging_client.return_value.project = "project1"

    mock_list_entries(mock_build_logging_client, entries)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(test_root_dir + "/bigquery/data/result.json")
