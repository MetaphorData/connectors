from pathlib import Path

import pytest
from google.cloud import bigquery

from metaphor.bigquery.table import TableExtractor
from tests.test_utils import load_json


@pytest.mark.parametrize(
    "table_type",
    [
        "TABLE",
        "VIEW",
        "MATERIALIZED_VIEW",
        "EXTERNAL",
        "SNAPSHOT",
    ],
)
def test_extract_table(test_root_dir: str, table_type: str):
    table_ref = bigquery.TableReference(
        dataset_ref=bigquery.DatasetReference(
            project="project",
            dataset_id="dataset1",
        ),
        table_id="table1",
    )
    table = bigquery.Table(
        table_ref=table_ref,
        schema=[
            bigquery.SchemaField("col1", "INT"),
            bigquery.SchemaField(
                "col2",
                "STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("col2_1", "TEXT", mode="REPEATED"),
                    bigquery.SchemaField("col2_1", "BOOL", mode="NULLABLE"),
                    bigquery.SchemaField("col2_3", "RECORD", mode="NULLABLE"),
                ],
            ),
        ],
    )
    table._properties["type"] = table_type  # type: ignore
    table.mview_query = "CREATE VIEW AS SELECT * FROM my-project.dataset.upstream"
    table.view_query = "CREATE VIEW AS SELECT * FROM my-project.dataset.upstream"
    table._properties["snapshotDefinition"] = {
        "snapshotTime": "2024-11-05T00:00:00.000Z"
    }  # :(
    schema = TableExtractor.extract_schema(table).to_dict()
    expected = (
        Path(test_root_dir)
        / "bigquery"
        / "data"
        / "tables"
        / f"{table_type.lower()}.json"
    )
    assert [schema] == load_json(expected)
