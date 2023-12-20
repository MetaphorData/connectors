import datetime
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.iam import User
from requests import HTTPError, Response

from metaphor.unity_catalog.config import UnityCatalogQueryLogConfig
from metaphor.unity_catalog.utils import (
    build_query_log_filter_by,
    escape_special_characters,
    list_column_lineage,
    list_table_lineage,
)


def test_list_table_lineage():
    client = MagicMock()
    client.do = MagicMock(
        return_value={
            "upstreams": [
                {
                    "tableInfo": {
                        "name": "upstream",
                        "catalog_name": "db",
                        "schema_name": "schema",
                    }
                },
                {
                    "tableInfo": {
                        "name": "upstream2",
                        "catalog_name": "db",
                        "schema_name": "schema",
                    },
                },
                {"fileInfo": {"path": "s3://path", "has_permission": True}},
                {"tableInfo": {"has_permission": False}},
            ],
            "downstreams": [],
        }
    )

    result = list_table_lineage(client, "table")

    assert not result.downstreams
    for upstream in result.upstreams:
        assert (upstream.fileInfo is not None) != (upstream.tableInfo is not None)


def test_list_table_lineage_unavailable_service():
    response = Response()
    response.status_code = 503

    client = MagicMock()
    client.do = MagicMock(side_effect=HTTPError(response=response))

    result = list_table_lineage(client, "foo")
    assert not result.upstreams
    assert not result.downstreams


def test_list_table_lineage_internal_server_error():
    response = Response()
    response.status_code = 500

    client = MagicMock()
    client.do = MagicMock(side_effect=HTTPError(response=response))

    with pytest.raises(HTTPError):
        list_table_lineage(client, "foo")


def test_list_column_lineage():
    client = MagicMock()
    client.do = MagicMock(
        return_value={
            "upstream_cols": [
                {
                    "name": "col1",
                    "catalog_name": "db",
                    "schema_name": "schema",
                    "table_name": "upstream",
                }
            ],
            "downstream_cols": [],
        }
    )

    result = list_column_lineage(client, "table", "col1")
    assert not result.downstream_cols
    assert all(col.name == "col1" for col in result.upstream_cols)


def test_list_column_lineage_unavailable_service():
    response = Response()
    response.status_code = 503

    client = MagicMock()
    client.do = MagicMock(side_effect=HTTPError(response=response))

    result = list_column_lineage(client, "foo", "bar")
    assert not result.upstream_cols
    assert not result.downstream_cols


def test_list_column_lineage_internal_server_error():
    response = Response()
    response.status_code = 500

    client = MagicMock()
    client.do = MagicMock(side_effect=HTTPError(response=response))

    with pytest.raises(HTTPError):
        list_column_lineage(client, "foo", "bar")


def test_parse_query_log_filter_by():
    client = MagicMock()
    client.users = MagicMock()
    client.users.list = lambda: [
        User(id="u0", user_name="alice"),
        User(id="u1", user_name="bob"),
        User(id="u2", user_name="charlie"),
        User(id="u3", user_name="dave"),
    ]
    config = UnityCatalogQueryLogConfig(
        lookback_days=2,
        excluded_usernames={"bob", "charlie"},
    )
    query_filter = build_query_log_filter_by(config, client)
    assert sorted(query_filter.user_ids) == ["u0", "u3"]
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
