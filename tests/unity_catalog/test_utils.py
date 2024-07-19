import datetime
from unittest.mock import MagicMock

from databricks.sdk.service.iam import User

from metaphor.unity_catalog.config import UnityCatalogQueryLogConfig
from metaphor.unity_catalog.models import Column, ColumnLineage, TableLineage
from metaphor.unity_catalog.utils import (
    build_query_log_filter_by,
    escape_special_characters,
    list_column_lineage,
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


def test_parse_query_log_filter_by():
    client = MagicMock()
    client.users = MagicMock()
    client.users.list = lambda: [
        User(id="0", user_name="alice"),
        User(id="1", user_name="bob"),
        User(id="2", user_name="charlie"),
        User(id="3", user_name="dave"),
    ]
    config = UnityCatalogQueryLogConfig(
        lookback_days=2,
        excluded_usernames={"bob", "charlie"},
    )
    query_filter = build_query_log_filter_by(config, client)
    assert sorted(query_filter.user_ids) == [0, 3]
    assert query_filter.query_start_time_range.start_time_ms is not None
    start_time = datetime.datetime.fromtimestamp(
        timestamp=query_filter.query_start_time_range.start_time_ms / 1000,
        tz=datetime.timezone.utc,
    )
    assert query_filter.query_start_time_range.end_time_ms is not None
    end_time = datetime.datetime.fromtimestamp(
        timestamp=query_filter.query_start_time_range.end_time_ms / 1000,
        tz=datetime.timezone.utc,
    )
    time_diff = end_time - start_time
    assert time_diff == datetime.timedelta(days=2)


def test_escape_special_characters():
    assert escape_special_characters("this.is.a_table") == "this.is.a_table"
    assert escape_special_characters("this.is.also-a-table") == "`this.is.also-a-table`"
