from unittest.mock import MagicMock

from metaphor.tableau.graphql_utils import paginate_connection


def test_paginate_connection():

    batch1 = {"data": {"someConnection": {"nodes": [{"val": 1}, {"val": 2}]}}}

    batch2 = {"data": {"someConnection": {"nodes": [{"val": 3}]}}}

    server = MagicMock()
    server.metadata = MagicMock()
    server.metadata.query.side_effect = [batch1, batch2]

    assert paginate_connection(server, "query", "someConnection", batch_size=2) == [
        {"val": 1},
        {"val": 2},
        {"val": 3},
    ]
