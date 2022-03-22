from unittest.mock import patch

import pytest
from freezegun import freeze_time

from metaphor.bigquery.query.config import BigQueryQueryRunConfig
from metaphor.bigquery.query.extractor import BigQueryQueryExtractor
from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from tests.bigquery.load_entries import load_entries
from tests.test_utils import load_json


def mock_list_entries(mock_build_log_client, entries):
    def side_effect(page_size, filter_):
        return entries

    mock_build_log_client.return_value.list_entries.side_effect = side_effect


@pytest.mark.asyncio
@freeze_time("2022-01-27")
async def test_extractor(test_root_dir):
    config = BigQueryQueryRunConfig(output=OutputConfig(), key_path="fake_file")
    extractor = BigQueryQueryExtractor()

    entries = load_entries(test_root_dir + "/bigquery/query/data/sample_log.json")

    # @patch doesn't work for async func in py3.7: https://bugs.python.org/issue36996
    with patch(
        "metaphor.bigquery.query.extractor.build_logging_client"
    ) as mock_build_client:
        mock_build_client.return_value.project = "project1"
        mock_list_entries(mock_build_client, entries)

        events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(test_root_dir + "/bigquery/query/data/result.json")
