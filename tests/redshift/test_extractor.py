import datetime
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.redshift.access_event import AccessEvent
from metaphor.redshift.config import RedshiftRunConfig
from metaphor.redshift.extractor import RedshiftExtractor
from tests.test_utils import load_json, wrap_query_log_stream_to_event


def mock_databases(mock: MagicMock, databases: List[str]):
    mock.return_value = databases


def dummy_config(**args):
    return RedshiftRunConfig(
        host="", database="", user="", password="", output=OutputConfig(), **args
    )


@patch(
    "metaphor.postgresql.extractor.BasePostgreSQLExtractor._fetch_databases",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_extractor(
    mock_fetch_databases: MagicMock,
):
    mock_databases(mock_fetch_databases, [])
    extractor = RedshiftExtractor(dummy_config())

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == []


def test_dataset_platform():
    extractor = RedshiftExtractor(dummy_config())
    assert extractor._dataset_platform == DataPlatform.REDSHIFT


def test_collect_query_logs(test_root_dir: str) -> None:
    # Random stuff generated with polyfactory
    access_events = [
        AccessEvent(
            user_id=7610,
            query_id=7086,
            usename="aZUytYDMFTryuKyWtbEM",
            rows=7155,
            bytes=6500,
            querytxt="select * from schema1.table1",
            database="db1",
            start_time=datetime.datetime(2010, 6, 26, 10, 7, 18, 14673),
            end_time=datetime.datetime(1999, 10, 8, 12, 25, 39, 706365),
        ),
        AccessEvent(
            user_id=8902,
            query_id=9910,
            usename="IevfvBUzEVUDrTbaIWKY",
            rows=4617,
            bytes=176,
            querytxt="select * from schema2.table2",
            database="db2",
            start_time=datetime.datetime(2003, 10, 1, 10, 15, 51, 904151),
            end_time=datetime.datetime(1996, 11, 14, 17, 6, 9, 676224),
        ),
    ]

    class AsyncIterator:
        def __init__(self) -> None:
            self.iter = iter(access_events)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.iter)
            except StopIteration:
                raise StopAsyncIteration

    included_dbs = {ae.database for ae in access_events}

    with patch(
        "metaphor.postgresql.extractor.BasePostgreSQLExtractor._connect_database"
    ) as mock_connect_database, patch(
        "metaphor.redshift.access_event.AccessEvent.fetch_access_event"
    ) as mock_fetch_access_event:
        mock_connect_database.return_value = MagicMock()
        mock_fetch_access_event.return_value = AsyncIterator()

        extractor = RedshiftExtractor(dummy_config())
        extractor._included_databases = included_dbs
        extractor._datasets["db1.schema1.table1"] = 1
        extractor._datasets["db2.schema2.table2"] = 1
        query_logs = wrap_query_log_stream_to_event(extractor.collect_query_logs())
        expected = f"{test_root_dir}/redshift/query_logs.json"
        assert query_logs == load_json(expected)
