from typing import List
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.redshift.lineage.config import RedshiftLineageRunConfig
from metaphor.redshift.lineage.extractor import RedshiftLineageExtractor
from tests.test_utils import AsyncMock, load_json


def mock_databases(mock: MagicMock, databases: List[str]):
    mock.return_value = databases


def mock_records(mock: MagicMock, records: List):
    mock_conn = AsyncMock()
    mock_conn.fetch.return_value = records
    mock_conn.close.return_value = None
    mock.return_value = mock_conn


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):
    config = RedshiftLineageRunConfig(  # nosec
        output=OutputConfig(),
        host="",
        database="",
        user="",
        password="",
        enable_view_lineage=False,
    )

    records = [
        {
            "target_schema": "private",
            "target_table": "t1",
            "source_schema": "public",
            "source_table": "s1",
            "querytxt": "q1",
        },
        {
            "target_schema": "private",
            "target_table": "t1",
            "source_schema": "public",
            "source_table": "s2",
            "querytxt": "q1",
        },
        {
            "target_schema": "foo",
            "target_table": "t2",
            "source_schema": "public",
            "source_table": "s1",
            "querytxt": "q2",
        },
        {
            "target_schema": "foo",
            "target_table": "t3",
            "source_schema": "public",
            "source_table": "s1",
            "querytxt": None,
        },
        {
            "target_schema": "foo",
            "target_table": "t4",
            "source_schema": "public",
            "source_table": "s1",
            "querytxt": "",
        },
        {
            "target_schema": "foo",
            "target_table": "t5",
            "source_schema": "public",
            "source_table": "s1",
        },
    ]

    with patch(
        "metaphor.postgresql.extractor.PostgreSQLExtractor._fetch_databases",
        new_callable=AsyncMock,
    ) as mock_fetch_databases, patch(
        "metaphor.postgresql.extractor.PostgreSQLExtractor._connect_database",
        new_callable=AsyncMock,
    ) as mock_connect_database:
        mock_databases(mock_fetch_databases, ["test"])
        mock_records(mock_connect_database, records)

        extractor = RedshiftLineageExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(test_root_dir + "/redshift/lineage/data/result.json")


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor_view(test_root_dir):
    config = RedshiftLineageRunConfig(  # nosec
        output=OutputConfig(),
        host="",
        database="",
        user="",
        password="",
        enable_lineage_from_stl_scan=False,
    )
    records = [
        {
            "target_schema": "private",
            "target_table": "t1",
            "source_schema": "public",
            "source_table": "s1",
        },
        {
            "target_schema": "private",
            "target_table": "t1",
            "source_schema": "public",
            "source_table": "s2",
        },
        {
            "target_schema": "foo",
            "target_table": "t2",
            "source_schema": "public",
            "source_table": "s1",
        },
    ]

    with patch(
        "metaphor.postgresql.extractor.PostgreSQLExtractor._fetch_databases",
        new_callable=AsyncMock,
    ) as mock_fetch_databases, patch(
        "metaphor.postgresql.extractor.PostgreSQLExtractor._connect_database",
        new_callable=AsyncMock,
    ) as mock_connect_database:
        mock_databases(mock_fetch_databases, ["test"])
        mock_records(mock_connect_database, records)

        extractor = RedshiftLineageExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(
        test_root_dir + "/redshift/lineage/data/result_view.json"
    )
