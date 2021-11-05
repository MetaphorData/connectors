from datetime import datetime, timezone
from typing import List

from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetUsage,
    FieldQueryCount,
    QueryCount,
    QueryCounts,
)

from metaphor.common.event_util import EventUtil
from metaphor.snowflake.filter import SnowflakeFilter
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


def test_table_percentile():
    dataset1 = make_dataset_with_usage([1, 3, 6, 8, 10])
    dataset2 = make_dataset_with_usage([2, 3, 5, 10, 20])
    dataset3 = make_dataset_with_usage([10, 20, 30, 40, 50])

    SnowflakeUsageExtractor.calculate_table_percentile(
        [dataset1, dataset2, dataset3],
        lambda dataset: dataset.usage.query_counts.last24_hours,
    )

    SnowflakeUsageExtractor.calculate_table_percentile(
        [dataset1, dataset2, dataset3],
        lambda dataset: dataset.usage.query_counts.last7_days,
    )

    # check the calculated percentile for 24 hours
    assert dataset1.usage.query_counts.last24_hours == QueryCount(
        count=1, percentile=0.33333333333333337
    )
    assert dataset2.usage.query_counts.last24_hours == QueryCount(
        count=2, percentile=0.6666666666666667
    )
    assert dataset3.usage.query_counts.last24_hours == QueryCount(
        count=10, percentile=1.0
    )

    # check the calculated percentile for 7 days
    assert dataset1.usage.query_counts.last7_days == QueryCount(
        count=3, percentile=0.6666666666666667
    )
    assert dataset2.usage.query_counts.last7_days == QueryCount(
        count=3, percentile=0.6666666666666667
    )
    assert dataset3.usage.query_counts.last7_days == QueryCount(
        count=20, percentile=1.0
    )

    # the percentile will be None since we didn't call the calculation for 30 days
    assert dataset1.usage.query_counts.last30_days == QueryCount(count=6)
    assert dataset2.usage.query_counts.last30_days == QueryCount(count=5)
    assert dataset3.usage.query_counts.last30_days == QueryCount(count=30)


def test_table_percentile_only_1():
    dataset1 = make_dataset_with_usage([1, 2, 3, 4, 5])

    SnowflakeUsageExtractor.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last24_hours
    )
    SnowflakeUsageExtractor.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last7_days
    )
    SnowflakeUsageExtractor.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last30_days
    )
    SnowflakeUsageExtractor.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last90_days
    )
    SnowflakeUsageExtractor.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last365_days
    )

    assert dataset1.usage.query_counts.last24_hours == QueryCount(
        count=1, percentile=1.0
    )
    assert dataset1.usage.query_counts.last7_days == QueryCount(count=2, percentile=1.0)
    assert dataset1.usage.query_counts.last30_days == QueryCount(
        count=3, percentile=1.0
    )
    assert dataset1.usage.query_counts.last90_days == QueryCount(
        count=4, percentile=1.0
    )
    assert dataset1.usage.query_counts.last365_days == QueryCount(
        count=5, percentile=1.0
    )


def test_column_percentile():
    columns: List[FieldQueryCount] = [
        FieldQueryCount(count=3),
        FieldQueryCount(count=1),
        FieldQueryCount(count=5),
    ]

    SnowflakeUsageExtractor.calculate_column_percentile(columns)

    assert columns == [
        FieldQueryCount(count=3, percentile=0.6666666666666667),
        FieldQueryCount(count=1, percentile=0.33333333333333337),
        FieldQueryCount(count=5, percentile=1.0),
    ]


def test_parse_access_log(test_root_dir):
    extractor = SnowflakeUsageExtractor()
    extractor.filter = SnowflakeFilter()

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

    extractor._parse_access_log(
        datetime.now().replace(tzinfo=timezone.utc), accessed_objects
    )

    results = {}
    for key, value in extractor._datasets.items():
        results[key] = EventUtil.clean_nones(value.to_dict())

    assert results == load_json(
        test_root_dir + "/snowflake/usage/data/parse_query_log_result.json"
    )
