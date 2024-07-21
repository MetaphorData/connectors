from datetime import datetime, timezone
from unittest.mock import MagicMock

from databricks.sdk.service.iam import ServicePrincipal
from freezegun import freeze_time
from pytest_snapshot.plugin import Snapshot

from metaphor.common.event_util import EventUtil
from metaphor.models.metadata_change_event import Dataset, DatasetStructure, QueryLogs
from metaphor.unity_catalog.models import Column, ColumnLineage, TableLineage
from metaphor.unity_catalog.utils import (
    batch_get_last_refreshed_time,
    escape_special_characters,
    get_last_refreshed_time,
    get_query_logs,
    list_column_lineage,
    list_query_logs,
    list_service_principals,
    list_table_lineage,
)
from tests.test_utils import load_json
from tests.unity_catalog.mocks import mock_connection_pool, mock_sql_connection


def test_get_query_logs(test_root_dir):

    mock_cursor = MagicMock()
    mock_cursor.fetchall = MagicMock()
    mock_cursor.fetchall.return_value = [
        {
            "query_id": "query1",
            "query_type": None,
            "query_text": "create table db.sch.tgt as select * from db.sch.src",
            "start_time": datetime.fromisoformat("2024-01-01T12:00:00"),
            "duration": 123,
            "rows_read": 456,
            "rows_written": 456,
            "bytes_read": 789,
            "bytes_written": 789,
            "email": "john.doe@metaphor.io",
        },
        {
            "query_id": "query2",
            "query_type": "CREATE",
            "query_text": """
CREATE TABLE monthly_sales_summary AS
SELECT
    DATE_TRUNC('month', o.order_date) AS month,
    SUM(oi.quantity * oi.price) AS total_sales,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM
    orders o
JOIN
    order_items oi ON o.order_id = oi.order_id
WHERE
    o.order_status = 'completed'
GROUP BY
    DATE_TRUNC('month', o.order_date);
            """,
            "start_time": datetime.fromisoformat("2024-02-01T12:00:00"),
            "duration": 12345,
            "rows_read": 45678,
            "rows_written": 45691,
            "bytes_read": 78922,
            "bytes_written": 78911,
            "email": "jane.doe@metaphor.io",
        },
    ]

    mock_cursor_ctx = MagicMock()
    mock_cursor_ctx.__enter__ = MagicMock()
    mock_cursor_ctx.__enter__.return_value = mock_cursor

    mock_connection = MagicMock()
    mock_connection.cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor_ctx

    logs = get_query_logs(
        mock_connection,
        1,
        set(),
        {
            "mydb.myschema.orders": Dataset(
                structure=DatasetStructure(
                    database="myDb",
                    schema="mySchema",
                    table="orders",
                ),
            ),
            "mydb.myschema.order_items": Dataset(
                structure=DatasetStructure(
                    database="myDb",
                    schema="mySchema",
                    table="order_items",
                ),
            ),
            "mydb.myschema.monthly_sales_summary": Dataset(
                structure=DatasetStructure(
                    database="myDb",
                    schema="mySchema",
                    table="monthly_sales_summary",
                ),
            ),
        },
    )
    logs = sorted((log for log in logs), key=lambda log: log.id or "")
    query_logs = EventUtil.build_then_trim(QueryLogs(logs))
    expected_query_logs = f"{test_root_dir}/unity_catalog/expected_query_logs.json"
    assert query_logs == load_json(expected_query_logs)


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

    list_query_logs(mock_connection, 1, ["user1", "user2"])

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
