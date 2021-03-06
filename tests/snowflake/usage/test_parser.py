from typing import List

from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetUsage,
    QueryCount,
    QueryCounts,
)

from metaphor.common.event_util import EventUtil
from metaphor.common.filter import DatasetFilter
from metaphor.common.utils import start_of_day
from metaphor.snowflake.usage.extractor import SnowflakeUsageExtractor
from tests.test_utils import load_json


def make_dataset_with_usage(counts: List[int]):
    dataset = Dataset()
    dataset.usage = DatasetUsage()
    dataset.usage.query_counts = QueryCounts(
        last24_hours=QueryCount(count=counts[0]),
        last7_days=QueryCount(count=counts[1]),
        last30_days=QueryCount(count=counts[2]),
        last90_days=QueryCount(count=counts[3]),
        last365_days=QueryCount(count=counts[4]),
    )
    return dataset


def test_parse_access_log(test_root_dir):
    extractor = SnowflakeUsageExtractor()
    extractor._utc_now = start_of_day()
    extractor._use_history = False
    extractor.filter = DatasetFilter()

    accessed_objects = """
    [
        {
            "columns": [
                {
                    "columnId": 1419851,
                    "columnName": "START_TIME"
                },
                {
                    "columnId": 1419832,
                    "columnName": "QUERY_ID"
                },
                {
                    "columnId": 1419833,
                    "columnName": "QUERY_TEXT"
                }
            ],
            "objectDomain": "View",
            "objectId": 1109388,
            "objectName": "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"
        },
        {
            "columns": [
                {
                    "columnId": 1511734,
                    "columnName": "DIRECT_OBJECTS_ACCESSED"
                },
                {
                    "columnId": 1511731,
                    "columnName": "QUERY_ID"
                }
            ],
            "objectDomain": "View",
            "objectId": 1154686,
            "objectName": "SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY"
        }
    ]
    """

    extractor._parse_access_log(start_of_day(), "tester", accessed_objects)

    results = {}
    for key, value in extractor._datasets.items():
        results[key] = EventUtil.clean_nones(value.to_dict())

    assert results == load_json(
        test_root_dir + "/snowflake/usage/data/parse_query_log_result.json"
    )
