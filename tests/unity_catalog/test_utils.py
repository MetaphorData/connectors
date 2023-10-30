from unittest.mock import MagicMock

import pytest
from requests import HTTPError, Response

from metaphor.unity_catalog.utils import list_column_lineage, list_table_lineage


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
