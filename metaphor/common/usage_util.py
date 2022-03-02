from datetime import datetime, timedelta
from typing import Callable, Collection, List, Optional

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
)


class UsageUtil:
    @staticmethod
    def init_dataset(
        account: Optional[str],
        full_name: str,
        platform: DataPlatform,
        useHistory: bool,
        utc_now: datetime,
    ) -> Dataset:
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=full_name, account=account, platform=platform
        )

        # write to dataset usage history
        if useHistory:
            dataset.usage_history = DatasetUsageHistory(
                history_type=HistoryType.DATASET_USAGE_HISTORY,
                history_date=utc_now,
                query_count=QueryCount(count=0.0, percentile=0.0),
                field_query_counts=[],
            )
            return dataset

        # write to dataset usage aspect
        dataset.usage = DatasetUsage(aspect_type=AspectType.DATASET_USAGE)
        dataset.usage.query_counts = QueryCounts(
            # quicktype bug: if use integer 0, "to_dict" will throw AssertionError as it expect float
            # See https://github.com/quicktype/quicktype/issues/1375
            last24_hours=QueryCount(count=0.0),
            last7_days=QueryCount(count=0.0),
            last30_days=QueryCount(count=0.0),
            last90_days=QueryCount(count=0.0),
            last365_days=QueryCount(count=0.0),
        )
        dataset.usage.field_query_counts = FieldQueryCounts(
            last24_hours=[],
            last7_days=[],
            last30_days=[],
            last90_days=[],
            last365_days=[],
        )

        return dataset

    @staticmethod
    def update_table_and_columns_usage(
        usage: DatasetUsage,
        columns: List[str],
        start_time: datetime,
        utc_now: datetime,
    ):
        if start_time > utc_now - timedelta(1):
            usage.query_counts.last24_hours.count += 1

        if start_time > utc_now - timedelta(7):
            usage.query_counts.last7_days.count += 1

        if start_time > utc_now - timedelta(30):
            usage.query_counts.last30_days.count += 1

        if start_time > utc_now - timedelta(90):
            usage.query_counts.last90_days.count += 1

        if start_time > utc_now - timedelta(365):
            usage.query_counts.last365_days.count += 1

        for column_name in columns:
            if start_time > utc_now - timedelta(1):
                UsageUtil.update_field_query_count(
                    usage.field_query_counts.last24_hours,
                    column_name,
                )

            if start_time > utc_now - timedelta(7):
                UsageUtil.update_field_query_count(
                    usage.field_query_counts.last7_days,
                    column_name,
                )

            if start_time > utc_now - timedelta(30):
                UsageUtil.update_field_query_count(
                    usage.field_query_counts.last30_days,
                    column_name,
                )

            if start_time > utc_now - timedelta(90):
                UsageUtil.update_field_query_count(
                    usage.field_query_counts.last90_days,
                    column_name,
                )

            if start_time > utc_now - timedelta(365):
                UsageUtil.update_field_query_count(
                    usage.field_query_counts.last365_days,
                    column_name,
                )

    @staticmethod
    def update_table_and_columns_usage_history(
        history: DatasetUsageHistory,
        columns: List[str],
    ):
        history.query_count.count += 1
        for column_name in columns:
            UsageUtil.update_field_query_count(history.field_query_counts, column_name)

    @staticmethod
    def update_field_query_count(query_counts: List[FieldQueryCount], column: str):
        item = next((x for x in query_counts if x.field == column), None)
        if item:
            item.count += 1
        else:
            query_counts.append(FieldQueryCount(field=column, count=1.0))

    @staticmethod
    def calculate_statistics(datasets: Collection[Dataset]) -> None:
        """Calculate statistics for the extracted usage info"""

        UsageUtil.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last24_hours
        )

        UsageUtil.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last7_days
        )

        UsageUtil.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last30_days
        )

        UsageUtil.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last90_days
        )

        UsageUtil.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last365_days
        )

        for dataset in datasets:
            UsageUtil.calculate_column_percentile(
                dataset.usage.field_query_counts.last24_hours
            )
            UsageUtil.calculate_column_percentile(
                dataset.usage.field_query_counts.last7_days
            )
            UsageUtil.calculate_column_percentile(
                dataset.usage.field_query_counts.last30_days
            )
            UsageUtil.calculate_column_percentile(
                dataset.usage.field_query_counts.last90_days
            )
            UsageUtil.calculate_column_percentile(
                dataset.usage.field_query_counts.last365_days
            )

    @staticmethod
    def calculate_table_percentile(
        datasets: Collection[Dataset], get_query_count: Callable[[Dataset], QueryCount]
    ) -> None:
        counts = [
            get_query_count(dataset).count
            for dataset in datasets
            if get_query_count(dataset).count > 0
        ]
        counts.sort()
        reverse_counts = counts[::-1]

        for dataset in datasets:
            query_count = get_query_count(dataset)
            query_count.percentile = UsageUtil._calculate_percentile(
                counts, reverse_counts, query_count.count
            )

    @staticmethod
    def calculate_column_percentile(columns: List[FieldQueryCount]) -> None:
        counts = [column.count for column in columns if column.count > 0]
        counts.sort()
        reverse_counts = counts[::-1]

        for column in columns:
            column.percentile = UsageUtil._calculate_percentile(
                counts, reverse_counts, column.count
            )

    @staticmethod
    def _calculate_percentile(
        sorted_list: List[float], reverse_sorted_list: List[float], score: float
    ) -> float:
        if score == 0:
            return 0.0
        left = sorted_list.index(score)
        right = reverse_sorted_list.index(score)
        return 0.5 + (left - right + 1) / 2 / len(sorted_list)
