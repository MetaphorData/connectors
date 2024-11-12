from datetime import datetime

from databricks.sql.types import Row

from metaphor.unity_catalog.models import TableInfo


class TestModel:
    row = Row(
        catalog_name="catalog",
        schema_name="schema",
        table_name="table",
        table_type="TABLE",
        owner="john.doe@metaphor.io",
        table_comment="this is a comment",
        created_at=datetime(2000, 1, 1, 0, 0, 0, 0),
        created_by="john.doe@metaphor.io",
        updated_at=datetime(2000, 1, 1, 0, 0, 0, 0),
        updated_by="john.doe@metaphor.io",
        data_source_format="sql",
        view_definition="SELECT * FROM catalog.schema.source",
        storage_path="s3://bucket/foo.csv",
        tags=None,
        columns=None,
    )

    def test_table_info_convert_none(self) -> None:
        table_info = TableInfo.from_row(self.row)
        assert table_info.model_dump() == {
            "catalog_name": "catalog",
            "schema_name": "schema",
            "table_name": "table",
            "type": "TABLE",
            "owner": "john.doe@metaphor.io",
            "comment": "this is a comment",
            "created_at": datetime.fromisoformat("2000-01-01T00:00:00"),
            "created_by": "john.doe@metaphor.io",
            "updated_at": datetime.fromisoformat("2000-01-01T00:00:00"),
            "updated_by": "john.doe@metaphor.io",
            "view_definition": "SELECT * FROM catalog.schema.source",
            "storage_location": "s3://bucket/foo.csv",
            "data_source_format": "sql",
            "tags": [],
            "columns": [],
        }

    def test_table_info_with_column(self) -> None:
        columns = [
            {
                "column_name": "col1",
                "data_type": "TEXT",
                "data_precision": None,
                "is_nullable": "NO",
                "comment": None,
                "tags": [
                    {
                        "tag_name": "tag1",
                        "tag_value": "value1",
                    },
                    {
                        "tag_name": "tag2",
                        "tag_value": "value2",
                    },
                ],
            },
            {
                "column_name": "col2",
                "data_type": "INT",
                "data_precision": 2,
                "is_nullable": "YES",
                "comment": "This is a nullable int field",
                "tags": [
                    {
                        "tag_name": "tag1",
                        "tag_value": "value1",
                    },
                ],
            },
        ]
        row = Row(**{**self.row.asDict(), "columns": columns})
        table_info = TableInfo.from_row(row)

        assert table_info.model_dump() == {
            "catalog_name": "catalog",
            "schema_name": "schema",
            "table_name": "table",
            "type": "TABLE",
            "owner": "john.doe@metaphor.io",
            "comment": "this is a comment",
            "created_at": datetime.fromisoformat("2000-01-01T00:00:00"),
            "created_by": "john.doe@metaphor.io",
            "updated_at": datetime.fromisoformat("2000-01-01T00:00:00"),
            "updated_by": "john.doe@metaphor.io",
            "view_definition": "SELECT * FROM catalog.schema.source",
            "storage_location": "s3://bucket/foo.csv",
            "data_source_format": "sql",
            "tags": [],
            "columns": [
                {
                    "column_name": "col1",
                    "data_type": "TEXT",
                    "data_precision": None,
                    "is_nullable": False,
                    "comment": None,
                    "tags": [
                        {"key": "tag1", "value": "value1"},
                        {"key": "tag2", "value": "value2"},
                    ],
                },
                {
                    "column_name": "col2",
                    "data_type": "INT",
                    "data_precision": 2,
                    "is_nullable": True,
                    "comment": "This is a nullable int field",
                    "tags": [{"key": "tag1", "value": "value1"}],
                },
            ],
        }
