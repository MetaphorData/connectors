from datetime import datetime

from metaphor.models.metadata_change_event import QueryInfo

from metaphor.common.query_history import TableQueryHistoryHeap


def test_recent_queries():
    heap = TableQueryHistoryHeap(3)

    heap.store_recent_query("foo", datetime(2022, 1, 1), "a", "user")
    heap.store_recent_query("bar", datetime(2022, 1, 10), "b", "user")
    heap.store_recent_query("bar", datetime(2022, 1, 20), "c", "user")
    heap.store_recent_query("bar", datetime(2022, 1, 9), "d", "user")

    assert {table: queries for table, queries in heap.recent_queries()} == {
        "foo": [QueryInfo(query="a", issued_by="user", issued_at=datetime(2022, 1, 1))],
        "bar": [
            QueryInfo(query="d", issued_by="user", issued_at=datetime(2022, 1, 9)),
            QueryInfo(query="b", issued_by="user", issued_at=datetime(2022, 1, 10)),
            QueryInfo(query="c", issued_by="user", issued_at=datetime(2022, 1, 20)),
        ],
    }

    heap.store_recent_query("foo", datetime(2022, 1, 2), "e", "user")
    heap.store_recent_query("foo", datetime(2022, 1, 3), "f", "user")
    heap.store_recent_query("foo", datetime(2022, 1, 4), "g", "user")
    heap.store_recent_query("bar", datetime(2022, 1, 11), "h", "user")

    assert {table: queries for table, queries in heap.recent_queries()} == {
        "foo": [
            QueryInfo(query="e", issued_by="user", issued_at=datetime(2022, 1, 2)),
            QueryInfo(query="f", issued_by="user", issued_at=datetime(2022, 1, 3)),
            QueryInfo(query="g", issued_by="user", issued_at=datetime(2022, 1, 4)),
        ],
        "bar": [
            QueryInfo(query="b", issued_by="user", issued_at=datetime(2022, 1, 10)),
            QueryInfo(query="h", issued_by="user", issued_at=datetime(2022, 1, 11)),
            QueryInfo(query="c", issued_by="user", issued_at=datetime(2022, 1, 20)),
        ],
    }
