from unittest.mock import AsyncMock, patch

import pytest
from freezegun import freeze_time

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.redshift.lineage.config import RedshiftLineageRunConfig
from metaphor.redshift.lineage.extractor import RedshiftLineageExtractor
from tests.test_utils import load_json


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
    mock_conn = AsyncMock()
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
    ]
    mock_conn.fetch.side_effect = lambda sql: records

    patcher = patch(
        "metaphor.postgresql.extractor.PostgreSQLExtractor._fetch_databases",
        return_value=["test"],
    )
    patcher.start()
    patcher2 = patch(
        "metaphor.postgresql.extractor.PostgreSQLExtractor._connect_database",
        return_value=mock_conn,
    )
    patcher2.start()

    extractor = RedshiftLineageExtractor()

    events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

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
    mock_conn = AsyncMock()
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
    mock_conn.fetch.side_effect = lambda sql: records

    patcher = patch(
        "metaphor.postgresql.extractor.PostgreSQLExtractor._fetch_databases",
        return_value=["test"],
    )
    patcher.start()
    patcher2 = patch(
        "metaphor.postgresql.extractor.PostgreSQLExtractor._connect_database",
        return_value=mock_conn,
    )
    patcher2.start()

    extractor = RedshiftLineageExtractor()

    events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(
        test_root_dir + "/redshift/lineage/data/result_view.json"
    )
