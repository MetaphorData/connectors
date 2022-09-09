from collections import namedtuple
from datetime import datetime
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.redshift.access_event import AccessEvent
from metaphor.redshift.usage.config import RedshiftUsageRunConfig
from metaphor.redshift.usage.extractor import RedshiftUsageExtractor
from tests.test_utils import AsyncMock, load_json


def load_records(path):
    sample_records = load_json(path)
    records = []

    for resource in sample_records:
        Record = namedtuple(
            "Record",
            [
                "userid",
                "query",
                "usename",
                "tbl",
                "querytxt",
                "database",
                "schema",
                "table",
                "starttime",
                "endtime",
                "aborted",
            ],
        )
        resource["starttime"] = datetime.fromisoformat(resource["starttime"])
        resource["endtime"] = datetime.fromisoformat(resource["endtime"])
        record = Record(**resource)
        records.append(record)

    return records


class AsyncIter:
    def __init__(self, items):
        self.items = [AccessEvent.from_record(record) for record in items]

    async def __aiter__(self):
        for item in self.items:
            yield item


@pytest.mark.asyncio
@freeze_time("2022-01-17")
async def test_extractor_no_history(test_root_dir):
    config = RedshiftUsageRunConfig(
        output=OutputConfig(),
        host="",
        database="",
        user="",
        password="",
        use_history=False,
    )
    records = load_records(test_root_dir + "/redshift/usage/data/sample_log.json")

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch("asyncpg.connect", new_callable=AsyncMock), patch(
        "metaphor.redshift.access_event.AccessEvent.fetch_access_event",
    ) as mock_fetch_usage:
        mock_fetch_usage.return_value = AsyncIter(records)

        extractor = RedshiftUsageExtractor(config)
        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(test_root_dir + "/redshift/usage/data/result.json")


@pytest.mark.asyncio
@freeze_time("2022-01-17")
async def test_extractor_use_history(test_root_dir):
    config = RedshiftUsageRunConfig(
        output=OutputConfig(),
        host="",
        database="",
        user="",
        password="",
    )
    records = load_records(test_root_dir + "/redshift/usage/data/sample_log.json")

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch("asyncpg.connect", new_callable=AsyncMock), patch(
        "metaphor.redshift.access_event.AccessEvent.fetch_access_event",
    ) as mock_fetch_usage:
        mock_fetch_usage.return_value = AsyncIter(records)

        extractor = RedshiftUsageExtractor(config)
        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(
        test_root_dir + "/redshift/usage/data/result_use_history.json"
    )
