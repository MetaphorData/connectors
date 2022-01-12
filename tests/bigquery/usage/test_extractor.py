from unittest.mock import patch

import pytest
from freezegun import freeze_time

from metaphor.bigquery.usage.config import BigQueryUsageRunConfig
from metaphor.bigquery.usage.extractor import BigQueryUsageExtractor
from metaphor.common.event_util import EventUtil
from tests.bigquery.usage.load_entries import load_entries
from tests.test_utils import load_json


def mock_list_entries(mock_build_client, entries):
    def side_effect(page_size, filter_):
        return entries

    mock_build_client.return_value.list_entries.side_effect = side_effect


@pytest.mark.asyncio
@freeze_time("2022-01-10")
async def test_extractor(test_root_dir):
    config = BigQueryUsageRunConfig(output=None, key_path="fake_file")
    extractor = BigQueryUsageExtractor()

    entries = load_entries(test_root_dir + "/bigquery/usage/data/sample_log.json")

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch("metaphor.bigquery.usage.extractor.build_client") as mock_build_client:
        mock_build_client.return_value.project = "project1"
        mock_list_entries(mock_build_client, entries)

        events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(test_root_dir + "/bigquery/usage/data/result.json")
