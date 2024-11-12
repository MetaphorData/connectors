from datetime import datetime, timezone
from unittest.mock import MagicMock

from freezegun import freeze_time
from pytest_snapshot.plugin import Snapshot

from metaphor.unity_catalog.models import (
    CatalogInfo,
    Column,
    ColumnInfo,
    ColumnLineage,
    SchemaInfo,
    TableInfo,
    TableLineage,
    Tag,
    VolumeFileInfo,
    VolumeInfo,
)
from metaphor.unity_catalog.queries import (
    get_last_refreshed_time,
    get_table_properties,
    list_catalogs,
    list_column_lineage,
    list_query_logs,
    list_schemas,
    list_table_lineage,
    list_tables,
    list_volume_files,
    list_volumes,
)
from tests.unity_catalog.mocks import mock_sql_connection


def test_list_catalogs(
    test_root_dir: str,
    snapshot: Snapshot,
):
    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                {
                    "catalog_name": "catalog1",
                    "catalog_owner": "owner1",
                    "comment": "comment1",
                    "tags": [
                        {"tag_name": "tag1", "tag_value": "value1"},
                        {"tag_name": "tag2", "tag_value": "value2"},
                    ],
                },
                {
                    "catalog_name": "catalog2",
                    "catalog_owner": "owner2",
                    "comment": "comment2",
                    "tags": [],
                },
            ]
        ],
        None,
        mock_cursor,
    )

    catalogs = list_catalogs(mock_connection)

    assert catalogs == [
        CatalogInfo(
            catalog_name="catalog1",
            owner="owner1",
            comment="comment1",
            tags=[
                Tag(key="tag1", value="value1"),
                Tag(key="tag2", value="value2"),
            ],
        ),
        CatalogInfo(
            catalog_name="catalog2", owner="owner2", comment="comment2", tags=[]
        ),
    ]

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_catalogs.sql")

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    catalogs = list_catalogs(mock_connection)
    assert catalogs == []


def test_list_schemas(
    test_root_dir: str,
    snapshot: Snapshot,
):
    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                {
                    "catalog_name": "catalog1",
                    "schema_name": "schema1",
                    "schema_owner": "owner1",
                    "comment": "comment1",
                    "tags": [
                        {"tag_name": "tag1", "tag_value": "value1"},
                        {"tag_name": "tag2", "tag_value": "value2"},
                    ],
                },
                {
                    "catalog_name": "catalog1",
                    "schema_name": "schema2",
                    "schema_owner": "owner2",
                    "comment": "comment2",
                    "tags": [],
                },
            ]
        ],
        None,
        mock_cursor,
    )

    schemas = list_schemas(mock_connection, "catalog1")

    assert schemas == [
        SchemaInfo(
            catalog_name="catalog1",
            schema_name="schema1",
            owner="owner1",
            comment="comment1",
            tags=[
                Tag(key="tag1", value="value1"),
                Tag(key="tag2", value="value2"),
            ],
        ),
        SchemaInfo(
            catalog_name="catalog1",
            schema_name="schema2",
            owner="owner2",
            comment="comment2",
            tags=[],
        ),
    ]

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_schemas.sql")

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    schemas = list_schemas(mock_connection, "catalog1")
    assert schemas == []


def test_list_tables(
    test_root_dir: str,
    snapshot: Snapshot,
):
    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                {
                    "catalog_name": "catalog1",
                    "schema_name": "schema1",
                    "table_name": "table1",
                    "table_type": "TABLE",
                    "owner": "owner1",
                    "table_comment": "table_comment1",
                    "data_source_format": "PARQUET",
                    "storage_path": "location1",
                    "created_at": datetime(2000, 1, 1),
                    "created_by": "user1",
                    "updated_at": datetime(2001, 1, 1),
                    "updated_by": "user2",
                    "view_definition": "definition",
                    "tags": None,
                    "columns": [
                        {
                            "column_name": "column1",
                            "data_type": "data_type1",
                            "data_precision": 10,
                            "is_nullable": "YES",
                            "comment": "column_comment1",
                            "tags": None,
                        },
                        {
                            "column_name": "column2",
                            "data_type": "data_type2",
                            "data_precision": 20,
                            "is_nullable": "NO",
                            "comment": "column_comment2",
                            "tags": None,
                        },
                    ],
                },
            ],
        ],
        None,
        mock_cursor,
    )

    tables = list_tables(mock_connection, "catalog1", "schema1")

    assert tables == [
        TableInfo(
            catalog_name="catalog1",
            schema_name="schema1",
            table_name="table1",
            type="TABLE",
            owner="owner1",
            comment="table_comment1",
            data_source_format="PARQUET",
            storage_location="location1",
            created_at=datetime(2000, 1, 1),
            created_by="user1",
            updated_at=datetime(2001, 1, 1),
            updated_by="user2",
            view_definition="definition",
            columns=[
                ColumnInfo(
                    column_name="column1",
                    data_type="data_type1",
                    data_precision=10,
                    is_nullable=True,
                    comment="column_comment1",
                    tags=[],
                ),
                ColumnInfo(
                    column_name="column2",
                    data_type="data_type2",
                    data_precision=20,
                    is_nullable=False,
                    comment="column_comment2",
                    tags=[],
                ),
            ],
        ),
    ]

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_tables.sql")

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    tables = list_tables(mock_connection, "catalog1", "schema1")
    assert tables == []


def test_list_volumes(
    test_root_dir: str,
    snapshot: Snapshot,
):
    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                {
                    "volume_catalog": "catalog1",
                    "volume_schema": "schema1",
                    "volume_name": "volume1",
                    "volume_type": "MANAGED",
                    "volume_owner": "owner1",
                    "comment": "comment1",
                    "created": datetime(2000, 1, 1),
                    "created_by": "user1",
                    "last_altered": datetime(2001, 1, 1),
                    "last_altered_by": "user2",
                    "last_altered_by": "user2",
                    "storage_location": "location1",
                    "tags": [
                        {"tag_name": "tag1", "tag_value": "value1"},
                        {"tag_name": "tag2", "tag_value": "value2"},
                    ],
                },
            ]
        ],
        None,
        mock_cursor,
    )

    volumes = list_volumes(mock_connection, "catalog1", "schema1")

    assert volumes == [
        VolumeInfo(
            catalog_name="catalog1",
            schema_name="schema1",
            volume_name="volume1",
            full_name="catalog1.schema1.volume1",
            volume_type="MANAGED",
            owner="owner1",
            comment="comment1",
            created_at=datetime(2000, 1, 1),
            created_by="user1",
            updated_at=datetime(2001, 1, 1),
            updated_by="user2",
            storage_location="location1",
            tags=[
                Tag(key="tag1", value="value1"),
                Tag(key="tag2", value="value2"),
            ],
        ),
    ]

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_volumes.sql")

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    volumes = list_volumes(mock_connection, "catalog1", "schema1")
    assert volumes == []


def test_list_volume_files(
    test_root_dir: str,
    snapshot: Snapshot,
):
    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                {
                    "path": "path1",
                    "name": "name1",
                    "size": 100,
                    "modification_time": datetime(2000, 1, 1),
                },
            ]
        ],
        None,
        mock_cursor,
    )

    volume_files = list_volume_files(
        mock_connection,
        VolumeInfo(
            catalog_name="catalog1",
            schema_name="schema1",
            volume_name="volume1",
            full_name="catalog1.schema1.volume1",
            volume_type="MANAGED",
            owner="owner1",
            created_at=datetime(2000, 1, 1),
            created_by="user1",
            updated_at=datetime(2001, 1, 1),
            updated_by="user2",
            storage_location="location1",
            tags=[],
        ),
    )

    assert volume_files == [
        VolumeFileInfo(
            path="path1",
            name="name1",
            size=100,
            last_updated=datetime(2000, 1, 1),
        ),
    ]

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_volume_files.sql")


def test_list_table_lineage(
    test_root_dir: str,
    snapshot: Snapshot,
):
    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                ("c.s.t1", "c.s.t3"),
                ("c.s.t2", "c.s.t3"),
                ("c.s.t4", "c.s.t2"),
            ]
        ],
        None,
        mock_cursor,
    )

    table_lineage = list_table_lineage(mock_connection, "c", "s")

    assert table_lineage == {
        "c.s.t3": TableLineage(upstream_tables=["c.s.t1", "c.s.t2"]),
        "c.s.t2": TableLineage(upstream_tables=["c.s.t4"]),
    }

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_table_lineage.sql")

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    table_lineage = list_table_lineage(mock_connection, "c", "s")
    assert table_lineage == {}


def test_list_column_lineage(
    test_root_dir: str,
    snapshot: Snapshot,
):
    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                ("c.s.t1", "c1", "c.s.t3", "ca"),
                ("c.s.t1", "c2", "c.s.t3", "ca"),
                ("c.s.t1", "c3", "c.s.t3", "cb"),
                ("c.s.t2", "c4", "c.s.t3", "ca"),
                ("c.s.t3", "c5", "c.s.t2", "cc"),
            ]
        ],
        None,
        mock_cursor,
    )

    column_lineage = list_column_lineage(mock_connection, "catalog", "schema")

    assert column_lineage == {
        "c.s.t3": ColumnLineage(
            upstream_columns={
                "ca": [
                    Column(column_name="c1", table_name="c.s.t1"),
                    Column(column_name="c2", table_name="c.s.t1"),
                    Column(column_name="c4", table_name="c.s.t2"),
                ],
                "cb": [Column(column_name="c3", table_name="c.s.t1")],
            }
        ),
        "c.s.t2": ColumnLineage(
            upstream_columns={
                "cc": [Column(column_name="c5", table_name="c.s.t3")],
            }
        ),
    }

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_column_lineage.sql")

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    column_lineage = list_column_lineage(mock_connection, "c", "s")
    assert column_lineage == {}


@freeze_time("2000-01-02")
def test_list_query_logs(
    test_root_dir: str,
    snapshot: Snapshot,
):

    mock_cursor = MagicMock()
    mock_connection = mock_sql_connection(None, None, mock_cursor)

    list_query_logs(mock_connection, 1, ["user1", "user2"])

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_query_log.sql")
    assert args[1] == [
        datetime(2000, 1, 1, tzinfo=timezone.utc),
        datetime(2000, 1, 2, tzinfo=timezone.utc),
    ]

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    query_logs = list_query_logs(mock_connection, 1, ["user1", "user2"])
    assert query_logs == []


def test_get_last_refreshed_time(
    test_root_dir: str,
    snapshot: Snapshot,
):

    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                {
                    "operation": "SET TBLPROPERTIES",
                    "timestamp": datetime(2020, 1, 4),
                },
                {
                    "operation": "ADD CONSTRAINT",
                    "timestamp": datetime(2020, 1, 3),
                },
                {
                    "operation": "CHANGE COLUMN",
                    "timestamp": datetime(2020, 1, 2),
                },
                {
                    "operation": "WRITE",
                    "timestamp": datetime(2020, 1, 1),
                },
            ]
        ],
        None,
        mock_cursor,
    )

    result = get_last_refreshed_time(mock_connection, "db.schema.table", 50)

    assert result == ("db.schema.table", datetime(2020, 1, 1))

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "describe_history.sql")

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    result = get_last_refreshed_time(mock_connection, "db.schema.table", 50)
    assert result is None


def test_get_table_properties(
    test_root_dir: str,
    snapshot: Snapshot,
):

    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                {
                    "key": "key1",
                    "value": "value1",
                },
                {
                    "key": "key2",
                    "value": "value2",
                },
            ]
        ],
        None,
        mock_cursor,
    )

    result = get_table_properties(mock_connection, "db.schema.table")

    assert result == ("db.schema.table", {"key1": "value1", "key2": "value2"})

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "show_table_properties.sql")

    # Exception handling
    mock_connection = mock_sql_connection([], Exception("some error"))
    result = get_table_properties(mock_connection, "db.schema.table")
    assert result is None
