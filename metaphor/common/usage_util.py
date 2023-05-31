from datetime import datetime, timedelta
from typing import Callable, Collection, List, Optional

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUsage,
    FieldQueryCount,
    FieldQueryCounts,
    QueryCount,
    QueryCounts,
    UserQueryCount,
    UserQueryCounts,
)


class UsageUtil:
    @staticmethod
    def init_dataset(
        normalized_name: str,
        platform: DataPlatform,
        account: Optional[str] = None,
    ) -> Dataset:
        dataset = Dataset(
            logical_id=DatasetLogicalID(
                name=normalized_name, account=account, platform=platform
            ),
        )

        dataset.usage = DatasetUsage(
            query_counts=QueryCounts(
                # quicktype bug: if use integer 0, "to_dict" will throw AssertionError as it expect float
                # See https://github.com/quicktype/quicktype/issues/1375
                last24_hours=QueryCount(count=0.0),
                last7_days=QueryCount(count=0.0),
                last30_days=QueryCount(count=0.0),
                last90_days=QueryCount(count=0.0),
                last365_days=QueryCount(count=0.0),
            ),
            field_query_counts=FieldQueryCounts(
                last24_hours=[],
                last7_days=[],
                last30_days=[],
                last90_days=[],
                last365_days=[],
            ),
            user_query_counts=UserQueryCounts(
                last24_hours=[],
                last7_days=[],
                last30_days=[],
                last90_days=[],
                last365_days=[],
            ),
        )

        return dataset

    @staticmethod
    def update_table_and_columns_usage(
        usage: DatasetUsage,
        columns: List[str],
        start_time: datetime,
        utc_now: datetime,
        username: Optional[str],
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
            UsageUtil._update_field_query_counts(
                usage.field_query_counts, column_name, start_time, utc_now
            )

        if username:
            UsageUtil._update_user_query_counts(
                usage.user_query_counts, username, start_time, utc_now
            )

    @staticmethod
    def _update_field_query_counts(
        field_query_counts: FieldQueryCounts,
        column: str,
        start_time: datetime,
        utc_now: datetime,
    ):
        if start_time > utc_now - timedelta(1):
            UsageUtil._update_field_query_count(
                field_query_counts.last24_hours,
                column,
            )

        if start_time > utc_now - timedelta(7):
            UsageUtil._update_field_query_count(
                field_query_counts.last7_days,
                column,
            )

        if start_time > utc_now - timedelta(30):
            UsageUtil._update_field_query_count(
                field_query_counts.last30_days,
                column,
            )

        if start_time > utc_now - timedelta(90):
            UsageUtil._update_field_query_count(
                field_query_counts.last90_days,
                column,
            )

        if start_time > utc_now - timedelta(365):
            UsageUtil._update_field_query_count(
                field_query_counts.last365_days,
                column,
            )

    @staticmethod
    def _update_user_query_counts(
        user_query_counts: UserQueryCounts,
        username: str,
        start_time: datetime,
        utc_now: datetime,
    ):
        if start_time > utc_now - timedelta(1):
            UsageUtil._update_user_query_count(
                user_query_counts.last24_hours,
                username,
            )

        if start_time > utc_now - timedelta(7):
            UsageUtil._update_user_query_count(
                user_query_counts.last7_days,
                username,
            )

        if start_time > utc_now - timedelta(30):
            UsageUtil._update_user_query_count(
                user_query_counts.last30_days,
                username,
            )

        if start_time > utc_now - timedelta(90):
            UsageUtil._update_user_query_count(
                user_query_counts.last90_days,
                username,
            )

        if start_time > utc_now - timedelta(365):
            UsageUtil._update_user_query_count(
                user_query_counts.last365_days,
                username,
            )

    @staticmethod
    def _update_field_query_count(query_counts: List[FieldQueryCount], column: str):
        item = next((x for x in query_counts if x.field == column), None)
        if item:
            item.count += 1
        else:
            query_counts.append(FieldQueryCount(field=column, count=1.0))

    @staticmethod
    def _update_user_query_count(query_counts: List[UserQueryCount], username: str):
        item = next((x for x in query_counts if x.user == username), None)
        if item:
            item.count += 1
        else:
            query_counts.append(UserQueryCount(user=username, count=1.0))

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
