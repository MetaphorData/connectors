from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from freezegun import freeze_time

from metaphor.common.base_config import OutputConfig
from metaphor.models.metadata_change_event import DataPlatform, QueriedDataset, QueryLog
from metaphor.postgresql.config import PostgreSQLQueryLogConfig
from metaphor.postgresql.extractor import PostgreSQLExtractor, PostgreSQLRunConfig


def test_parse_max_length():
    assert PostgreSQLExtractor._parse_max_length("foo") is None
    assert PostgreSQLExtractor._parse_max_length("foo(a)") is None
    assert PostgreSQLExtractor._parse_max_length("foo(10)") == 10


def test_parse_precision():
    assert PostgreSQLExtractor._parse_precision("foo") is None
    assert PostgreSQLExtractor._parse_precision("foo(a,10)") is None
    assert PostgreSQLExtractor._parse_precision("foo(,10)") is None
    assert PostgreSQLExtractor._parse_precision("foo(10,b)") is None
    assert PostgreSQLExtractor._parse_precision("foo(10,)") is None
    assert PostgreSQLExtractor._parse_precision("foo(10,3)") == 10
    assert PostgreSQLExtractor._parse_precision("foo(10)") == 10


def test_parse_format_type():
    assert PostgreSQLExtractor._parse_format_type("foo", "foo") == (None, None)
    assert PostgreSQLExtractor._parse_format_type("integer", "foo") == (32.0, None)
    assert PostgreSQLExtractor._parse_format_type("smallint", "foo") == (16.0, None)
    assert PostgreSQLExtractor._parse_format_type("bigint", "foo") == (64.0, None)
    assert PostgreSQLExtractor._parse_format_type("real", "foo") == (24.0, None)
    assert PostgreSQLExtractor._parse_format_type("double precision", "foo") == (
        53.0,
        None,
    )
    assert PostgreSQLExtractor._parse_format_type("numeric", "foo") == (None, None)
    assert PostgreSQLExtractor._parse_format_type("numeric", "numeric(10,1)") == (
        10,
        None,
    )
    assert PostgreSQLExtractor._parse_format_type("numeric", "numeric") == (None, None)
    assert PostgreSQLExtractor._parse_format_type("character", "character(10)") == (
        None,
        10,
    )
    assert PostgreSQLExtractor._parse_format_type(
        "character varying", "character varying(10)"
    ) == (None, 10)


def test_extract_duration():
    assert PostgreSQLExtractor._extract_duration("1 ms") == 1.0
    assert PostgreSQLExtractor._extract_duration("1 second") is None


def dummy_config(**args):
    return PostgreSQLRunConfig(
        host="",
        database="",
        user="",
        password="",
        output=OutputConfig(),
        **args,
        query_log=PostgreSQLQueryLogConfig(excluded_usernames={"foo"}),
    )


gold = QueryLog(
    id="POSTGRESQL:330833d44bea62c26a5d6dd206eecdf4",
    account=None,
    bytes_read=None,
    bytes_written=None,
    cost=None,
    default_database="metaphor",
    default_schema=None,
    duration=55.66,
    email=None,
    metadata=None,
    parsing=None,
    platform=DataPlatform.POSTGRESQL,
    query_id="330833d44bea62c26a5d6dd206eecdf4",
    rows_read=None,
    rows_written=None,
    sources=[
        QueriedDataset(
            columns=None,
            database="metaphor",
            id="DATASET~F68D8D6F1F49DA4605F13F20FD3CA883",
            schema="schema",
            table="table",
        )
    ],
    sql="SELECT x, y from schema.table;",
    sql_hash="330833d44bea62c26a5d6dd206eecdf4",
    start_time=datetime(2024, 8, 29, 9, 25, 50, tzinfo=timezone.utc),
    targets=[],
    type=None,
    user_id="metaphor",
)


def test_process_cloud_watch_log():
    extractor = PostgreSQLExtractor(dummy_config())

    cache = {}

    # No previous log
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:50 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: duration: 0.858 ms",
            cache,
        )
        is None
    )

    # Valid Statement
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:50 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: statement: SELECT x, y from schema.table;",
            cache,
        )
        is None
    )
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:51 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: duration: 55.66 ms",
            cache,
        )
        == gold
    )

    # SQL don't have source and targets
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:50 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: statement: CREATE USER abc PASSWORD 'asjdkasjd';",
            cache,
        )
        is None
    )
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:51 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: duration: 55.66 ms",
            cache,
        )
        is None
    )

    # Skip queries in excluded_user
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:50 UTC:10.1.1.134(48507):foo@metaphor:[615]:LOG: statement: SELECT x, y from schema.table;",
            cache,
        )
        is None
    )
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:51 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: duration: 55.66 ms",
            cache,
        )
        is None
    )

    # Skip log that is not `statement` or `execute`
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:50 UTC:10.1.1.134(48507):foo@metaphor:[615]:LOG: bind: SELECT x, y from schema.table;",
            cache,
        )
        is None
    )
    assert (
        extractor._process_cloud_watch_log(
            "2024-08-29 09:25:51 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: duration: 55.66 ms",
            cache,
        )
        is None
    )

    # Skip log that is not start with date time
    assert (
        extractor._process_cloud_watch_log(
            "(1,2,3),(4,5,6)",
            cache,
        )
        is None
    )


@freeze_time("2000-01-01")
def test_collect_query_logs_from_cloud_watch():
    extractor = PostgreSQLExtractor(dummy_config())

    mocked_client = MagicMock()
    mocked_client.filter_log_events.side_effect = [
        {"nextToken": "token", "events": [{"message": ""}]},
        {
            "events": [
                {
                    "message": "2024-08-29 09:25:50 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: statement: SELECT x, y from schema.table;",
                },
                {
                    "message": "2024-08-29 09:25:51 UTC:10.1.1.134(48507):metaphor@metaphor:[615]:LOG: duration: 55.66 ms",
                },
            ]
        },
    ]

    iterator = extractor._collect_query_logs_from_cloud_watch(
        mocked_client, lookback_days=1, logs_group="123"
    )
    assert next(iterator) == gold

    with pytest.raises(StopIteration):
        next(iterator)
    mocked_client.filter_log_events.assert_has_calls(
        [
            call(
                logGroupName="123",
                startTime=946598400000,
                endTime=946684800000,
            ),
            call(
                logGroupName="123",
                startTime=946598400000,
                endTime=946684800000,
                nextToken="token",
            ),
        ]
    )


@patch(
    "metaphor.postgresql.extractor.PostgreSQLExtractor._fetch_databases",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_extractor(mocked_fetch_databases):
    mocked_fetch_databases.return_value = []
    extractor = PostgreSQLExtractor(dummy_config())
    events = [e for e in await extractor.extract()]
    assert events == []
