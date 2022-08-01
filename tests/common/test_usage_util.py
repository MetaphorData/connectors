from datetime import datetime
from typing import List

from metaphor.common.usage_util import UsageUtil
from metaphor.models.metadata_change_event import (
    AspectType,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUsage,
    DatasetUsageHistory,
    EntityType,
    FieldQueryCount,
    FieldQueryCounts,
    HistoryType,
    QueryCount,
    QueryCounts,
    UserQueryCounts,
)


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


def test_init_dataset():
    now = datetime.now()
    dataset1 = UsageUtil.init_dataset(None, "foo", DataPlatform.S3, False, now)
    assert dataset1 == Dataset(
        entity_type=EntityType.DATASET,
        logical_id=DatasetLogicalID(name="foo", platform=DataPlatform.S3),
        usage=DatasetUsage(
            aspect_type=AspectType.DATASET_USAGE,
            field_query_counts=FieldQueryCounts(
                last24_hours=[],
                last30_days=[],
                last365_days=[],
                last7_days=[],
                last90_days=[],
            ),
            query_counts=QueryCounts(
                last24_hours=QueryCount(count=0.0, percentile=None),
                last30_days=QueryCount(count=0.0, percentile=None),
                last365_days=QueryCount(count=0.0, percentile=None),
                last7_days=QueryCount(count=0.0, percentile=None),
                last90_days=QueryCount(count=0.0, percentile=None),
            ),
            user_query_counts=UserQueryCounts(
                last24_hours=[],
                last30_days=[],
                last365_days=[],
                last7_days=[],
                last90_days=[],
            ),
        ),
        usage_history=None,
    )

    dataset2 = UsageUtil.init_dataset(None, "bar", DataPlatform.S3, True, now)
    assert dataset2 == Dataset(
        entity_type=EntityType.DATASET,
        logical_id=DatasetLogicalID(name="bar", platform=DataPlatform.S3),
        usage_history=DatasetUsageHistory(
            field_query_counts=[],
            history_date=now,
            history_type=HistoryType.DATASET_USAGE_HISTORY,
            query_count=QueryCount(count=0.0, percentile=0.0),
            user_query_counts=[],
        ),
    )


def test_table_percentile():
    dataset1 = make_dataset_with_usage([1, 3, 6, 8, 10])
    dataset2 = make_dataset_with_usage([2, 3, 5, 10, 20])
    dataset3 = make_dataset_with_usage([10, 20, 30, 40, 50])

    UsageUtil.calculate_table_percentile(
        [dataset1, dataset2, dataset3],
        lambda dataset: dataset.usage.query_counts.last24_hours,
    )

    UsageUtil.calculate_table_percentile(
        [dataset1, dataset2, dataset3],
        lambda dataset: dataset.usage.query_counts.last7_days,
    )

    # check the calculated percentile for 24 hours
    assert dataset1.usage.query_counts.last24_hours == QueryCount(
        count=1, percentile=0.33333333333333337
    )
    assert dataset2.usage.query_counts.last24_hours == QueryCount(
        count=2, percentile=0.6666666666666666
    )
    assert dataset3.usage.query_counts.last24_hours == QueryCount(
        count=10, percentile=1.0
    )

    # check the calculated percentile for 7 days
    assert dataset1.usage.query_counts.last7_days == QueryCount(count=3, percentile=0.5)
    assert dataset2.usage.query_counts.last7_days == QueryCount(count=3, percentile=0.5)
    assert dataset3.usage.query_counts.last7_days == QueryCount(
        count=20, percentile=1.0
    )

    # the percentile will be None since we didn't call the calculation for 30 days
    assert dataset1.usage.query_counts.last30_days == QueryCount(count=6)
    assert dataset2.usage.query_counts.last30_days == QueryCount(count=5)
    assert dataset3.usage.query_counts.last30_days == QueryCount(count=30)


def test_table_percentile_only_1():
    dataset1 = make_dataset_with_usage([1, 2, 3, 4, 5])

    UsageUtil.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last24_hours
    )
    UsageUtil.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last7_days
    )
    UsageUtil.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last30_days
    )
    UsageUtil.calculate_table_percentile(
        [dataset1], lambda dataset: dataset.usage.query_counts.last90_days
    )
    UsageUtil.calculate_table_percentile(
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

    UsageUtil.calculate_column_percentile(columns)

    assert columns == [
        FieldQueryCount(count=3, percentile=0.6666666666666666),
        FieldQueryCount(count=1, percentile=0.33333333333333337),
        FieldQueryCount(count=5, percentile=1.0),
    ]


def test_column_percentile_with_duplicate():
    columns: List[FieldQueryCount] = [
        FieldQueryCount(count=3),
        FieldQueryCount(count=1),
        FieldQueryCount(count=3),
        FieldQueryCount(count=1),
        FieldQueryCount(count=5),
    ]

    UsageUtil.calculate_column_percentile(columns)

    assert columns == [
        FieldQueryCount(count=3, field=None, percentile=0.7),
        FieldQueryCount(count=1, field=None, percentile=0.3),
        FieldQueryCount(count=3, field=None, percentile=0.7),
        FieldQueryCount(count=1, field=None, percentile=0.3),
        FieldQueryCount(count=5, field=None, percentile=1.0),
    ]

    columns: List[FieldQueryCount] = [
        FieldQueryCount(count=1),
        FieldQueryCount(count=1),
        FieldQueryCount(count=1),
        FieldQueryCount(count=1),
        FieldQueryCount(count=5),
    ]

    UsageUtil.calculate_column_percentile(columns)

    assert columns == [
        FieldQueryCount(count=1, field=None, percentile=0.5),
        FieldQueryCount(count=1, field=None, percentile=0.5),
        FieldQueryCount(count=1, field=None, percentile=0.5),
        FieldQueryCount(count=1, field=None, percentile=0.5),
        FieldQueryCount(count=5, field=None, percentile=1.0),
    ]


def test_column_percentile_with_0():
    columns: List[FieldQueryCount] = [
        FieldQueryCount(count=0),
        FieldQueryCount(count=0),
        FieldQueryCount(count=1),
    ]

    UsageUtil.calculate_column_percentile(columns)

    assert columns == [
        FieldQueryCount(count=0, percentile=0),
        FieldQueryCount(count=0, percentile=0),
        FieldQueryCount(count=1, percentile=1.0),
    ]
