import heapq
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import Dict, List, Set

from metaphor.models.metadata_change_event import QueryInfo


@dataclass(order=True)
class PrioritizedQueryInfo:
    time: datetime
    item: QueryInfo = field(compare=False)


class TableQueryHistoryHeap:
    """
    A container class to store the N most recent unique queries for each table.
    """

    def __init__(self, max_queries_per_table):
        self._table_query_set: Dict[str, Set[str]] = {}
        self._table_queries: Dict[str, List[PrioritizedQueryInfo]] = {}
        self._max_queries_per_table = max_queries_per_table

    def store_recent_query(self, table_name: str, query_info: QueryInfo) -> None:
        """Store most recent N unique query for a table"""
        query_hash = sha256(query_info.query.encode("utf-8")).hexdigest()
        query_set = self._table_query_set.setdefault(table_name, set())
        if query_hash in query_set:
            return
        query_set.add(query_hash)

        query_heap = self._table_queries.setdefault(table_name, [])
        heapq.heappush(
            query_heap,
            PrioritizedQueryInfo(
                time=query_info.issued_at,
                item=query_info,
            ),
        )

        if len(query_heap) > self._max_queries_per_table:
            heapq.heappop(query_heap)

    def recent_queries(self):
        """Yield each table with sorted recent queries"""
        for table_name, query_heap in self._table_queries.items():
            yield table_name, [q.item for q in sorted(query_heap)]
