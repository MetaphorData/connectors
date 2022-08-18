from datetime import datetime, timezone
from unittest.mock import patch

from freezegun import freeze_time

from metaphor.bigquery.usage.config import BigQueryUsageRunConfig
from metaphor.bigquery.usage.extractor import BigQueryUsageExtractor
from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from tests.bigquery.load_entries import load_entries
from tests.test_utils import load_json


@freeze_time("2022-01-10")
def test_parse_access_log(test_root_dir):
    config = BigQueryUsageRunConfig(
        use_history=False, output=OutputConfig(), key_path="fake_file"
    )

    with patch("metaphor.bigquery.usage.extractor.build_logging_client"):
        extractor = BigQueryUsageExtractor(config)

        for entry in load_entries(
            test_root_dir + "/bigquery/usage/data/sample_log.json"
        ):
            extractor._parse_table_data_read_entry(entry, datetime.now(tz=timezone.utc))

        results = {}
        for key, value in extractor._datasets.items():
            results[key] = EventUtil.clean_nones(value.to_dict())

        assert results == load_json(
            test_root_dir + "/bigquery/usage/data/parse_query_log_result.json"
        )
