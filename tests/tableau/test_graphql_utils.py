from unittest.mock import MagicMock

from metaphor.tableau.graphql_utils import paginate_connection


def test_paginate_connection():

    batch1 = {"data": {"someConnection": {"nodes": [{"val": 1}]}}}

    batch2 = {"data": {"someConnection": {"nodes": [{"val": 2}]}}}

    batch3 = {"data": {"someConnection": {"nodes": []}}}

    server = MagicMock()
    server.metadata = MagicMock()
    server.metadata.query.side_effect = [batch1, batch2, batch3]

    assert paginate_connection(server, "query", "someConnection", batch_size=1) == [
        {"val": 1},
        {"val": 2},
    ]
