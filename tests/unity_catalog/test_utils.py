from datetime import datetime, timezone
from unittest.mock import MagicMock

from freezegun import freeze_time
from pytest_snapshot.plugin import Snapshot

from metaphor.unity_catalog.models import Column, ColumnLineage, TableLineage
from metaphor.unity_catalog.utils import (
    escape_special_characters,
    list_column_lineage,
    list_query_log,
    list_table_lineage,
)
from tests.unity_catalog.mocks import mock_sql_connection


def test_list_table_lineage():
    mock_connection = mock_sql_connection(
        [
            [
                ("c.s.t1", "c.s.t3"),
                ("c.s.t2", "c.s.t3"),
                ("c.s.t4", "c.s.t2"),
            ]
        ]
    )

    table_lineage = list_table_lineage(mock_connection, "c", "s")

    assert table_lineage == {
        "c.s.t3": TableLineage(upstream_tables=["c.s.t1", "c.s.t2"]),
        "c.s.t2": TableLineage(upstream_tables=["c.s.t4"]),
    }


def test_list_column_lineage():
    mock_connection = mock_sql_connection(
        [
            [
                ("c.s.t1", "c1", "c.s.t3", "ca"),
                ("c.s.t1", "c2", "c.s.t3", "ca"),
                ("c.s.t1", "c3", "c.s.t3", "cb"),
                ("c.s.t2", "c4", "c.s.t3", "ca"),
                ("c.s.t3", "c5", "c.s.t2", "cc"),
            ]
        ]
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


@freeze_time("2000-01-02")
def test_list_query_logs(
    test_root_dir: str,
    snapshot: Snapshot,
):

    mock_cursor = MagicMock()
    mock_connection = mock_sql_connection(None, None, mock_cursor)

    list_query_log(mock_connection, 1, ["user1", "user2"])

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "list_query_log.sql")
    assert args[1] == [
        datetime(2000, 1, 1, tzinfo=timezone.utc),
        datetime(2000, 1, 2, tzinfo=timezone.utc),
    ]


def test_escape_special_characters():
    assert escape_special_characters("this.is.a_table") == "this.is.a_table"
    assert escape_special_characters("this.is.also-a-table") == "`this.is.also-a-table`"
