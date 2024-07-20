from datetime import datetime, timezone
from unittest.mock import MagicMock

from databricks.sdk.service.iam import ServicePrincipal
from freezegun import freeze_time
from pytest_snapshot.plugin import Snapshot

from metaphor.unity_catalog.models import Column, ColumnLineage, TableLineage
from metaphor.unity_catalog.utils import (
    batch_get_last_refreshed_time,
    escape_special_characters,
    get_last_refreshed_time,
    list_column_lineage,
    list_query_log,
    list_service_principals,
    list_table_lineage,
)
from tests.unity_catalog.mocks import mock_connection_pool, mock_sql_connection


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


def test_batch_get_last_refreshed_time():

    connection_pool = mock_connection_pool(
        [
            [
                {
                    "operation": "WRITE",
                    "timestamp": datetime(2020, 1, 1),
                },
            ],
            [
                {
                    "operation": "WRITE",
                    "timestamp": datetime(2020, 1, 1),
                },
            ],
        ],
    )

    result_map = batch_get_last_refreshed_time(connection_pool, ["a.b.c", "d.e.f"], 10)

    assert result_map == {"a.b.c": datetime(2020, 1, 1), "d.e.f": datetime(2020, 1, 1)}


def test_list_service_principals():

    sp1 = ServicePrincipal(application_id="sp1", display_name="SP1")
    sp2 = ServicePrincipal(application_id="sp2", display_name="SP2")

    mock_api = MagicMock()
    mock_api.service_principals = MagicMock()
    mock_api.service_principals.list.return_value = [sp1, sp2]

    principals = list_service_principals(mock_api)

    assert principals == {"sp1": sp1, "sp2": sp2}


def test_escape_special_characters():
    assert escape_special_characters("this.is.a_table") == "this.is.a_table"
    assert escape_special_characters("this.is.also-a-table") == "`this.is.also-a-table`"
