import re

from freezegun import freeze_time

from metaphor.bigquery.usage.extractor import BigQueryUsageExtractor
from metaphor.common.event_util import EventUtil
from tests.bigquery.usage.load_entries import load_entries
from tests.test_utils import load_json


@freeze_time("2022-01-10")
def test_parse_access_log(test_root_dir):
    extractor = BigQueryUsageExtractor()
    extractor._datasets_pattern = [re.compile(".*")]

    for entry in load_entries(test_root_dir + "/bigquery/usage/data/sample_log.json"):
        extractor._parse_log_entry(entry)

    results = {}
    for key, value in extractor._datasets.items():
        results[key] = EventUtil.clean_nones(value.to_dict())

    assert results == load_json(
        test_root_dir + "/bigquery/usage/data/parse_query_log_result.json"
    )
