from datetime import datetime

from metaphor.common.query_history import TableQueryHistoryHeap
from metaphor.models.metadata_change_event import QueryInfo


def test_recent_queries():
    heap = TableQueryHistoryHeap(3)

    heap.store_recent_query(
        "foo", QueryInfo(query="a", issued_by="user", issued_at=datetime(2022, 1, 1))
    )
    heap.store_recent_query(
        "bar", QueryInfo(query="b", issued_by="user", issued_at=datetime(2022, 1, 10))
    )
    heap.store_recent_query(
        "bar", QueryInfo(query="c", issued_by="user", issued_at=datetime(2022, 1, 20))
    )
    heap.store_recent_query(
        "bar", QueryInfo(query="d", issued_by="user", issued_at=datetime(2022, 1, 9))
    )

    assert {table: queries for table, queries in heap.recent_queries()} == {
        "foo": [QueryInfo(query="a", issued_by="user", issued_at=datetime(2022, 1, 1))],
        "bar": [
            QueryInfo(query="d", issued_by="user", issued_at=datetime(2022, 1, 9)),
            QueryInfo(query="b", issued_by="user", issued_at=datetime(2022, 1, 10)),
            QueryInfo(query="c", issued_by="user", issued_at=datetime(2022, 1, 20)),
        ],
    }

    heap.store_recent_query(
        "foo", QueryInfo(query="e", issued_by="user", issued_at=datetime(2022, 1, 2))
    )
    heap.store_recent_query(
        "foo", QueryInfo(query="f", issued_by="user", issued_at=datetime(2022, 1, 3))
    )
    heap.store_recent_query(
        "foo", QueryInfo(query="g", issued_by="user", issued_at=datetime(2022, 1, 4))
    )
    heap.store_recent_query(
        "bar", QueryInfo(query="h", issued_by="user", issued_at=datetime(2022, 1, 11))
    )

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
