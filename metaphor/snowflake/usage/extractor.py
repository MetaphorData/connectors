import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Collection, Dict, List, Tuple

from metaphor.models.metadata_change_event import (
    AspectType,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUsage,
    EntityType,
    FieldQueryCount,
    FieldQueryCounts,
    MetadataChangeEvent,
    QueryCount,
    QueryCounts,
)
from serde import deserialize
from serde.json import from_json

from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.snowflake.auth import connect
from metaphor.snowflake.filter import DatabaseFilter
from metaphor.snowflake.usage.config import SnowflakeUsageRunConfig
from metaphor.snowflake.utils import QueryWithParam, async_execute, include_table

logger = get_logger(__name__)

# disable logging from sql_metadata
logging.getLogger("Parser").setLevel(logging.CRITICAL)


@deserialize
@dataclass
class AccessedObjectColumn:
    columnId: int
    columnName: str


@deserialize
@dataclass
class AccessedObject:
    objectDomain: str
    objectName: str
    objectId: int
    columns: List[AccessedObjectColumn]


DEFAULT_EXCLUDED_DATABASES: DatabaseFilter = {"SNOWFLAKE": None}


class SnowflakeUsageExtractor(BaseExtractor):
    """Snowflake usage metadata extractor"""

    @staticmethod
    def config_class():
        return SnowflakeUsageRunConfig

    def __init__(self):
        self.account = None
        self.filter = None
        self.max_concurrency = None
        self.batch_size = None
        self._datasets: Dict[str, Dataset] = {}

    async def extract(
        self, config: SnowflakeUsageRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeUsageExtractor.config_class())

        logger.info("Fetching usage info from Snowflake")

        self.account = config.account
        self.max_concurrency = config.max_concurrency
        self.batch_size = config.batch_size

        self.filter = config.filter.normalize()

        self.filter.excludes = (
            DEFAULT_EXCLUDED_DATABASES
            if self.filter.excludes is None
            else {**self.filter.excludes, **DEFAULT_EXCLUDED_DATABASES}
        )

        excluded_usernames_clause = (
            f"and USER_NAME NOT IN ({','.join(['%s'] * len(config.excluded_usernames))})"
            if len(config.excluded_usernames) > 0
            else ""
        )

        start_date = datetime.utcnow().date() - timedelta(config.lookback_days)

        conn = connect(config)

        with conn:
            cursor = conn.cursor()

            cursor.execute(
                f"""
                SELECT COUNT(1)
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE EXECUTION_STATUS = 'SUCCESS' and START_TIME > %s
                  {excluded_usernames_clause}
                """,
                (
                    start_date,
                    *config.excluded_usernames,
                ),
            )
            count = cursor.fetchone()[0]
            batches = math.ceil(count / self.batch_size)
            logger.info(f"Total {count} queries, dividing into {batches} batches")

            queries = {
                x: QueryWithParam(
                    f"""
                    SELECT q.QUERY_ID, START_TIME, DIRECT_OBJECTS_ACCESSED
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                    JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
                      ON a.QUERY_ID = q.QUERY_ID
                    WHERE EXECUTION_STATUS = 'SUCCESS' and START_TIME > %s
                      {excluded_usernames_clause}
                    ORDER BY q.QUERY_ID
                    LIMIT {self.batch_size} OFFSET %s
                    """,
                    (
                        start_date,
                        *config.excluded_usernames,
                        x * self.batch_size,
                    ),
                )
                for x in range(batches)
            }
            async_execute(
                conn,
                queries,
                "fetch_access_logs",
                self.max_concurrency,
                self._parse_access_logs,
            )

        # calculate statistics based on the counts
        self._calculate_statistics()

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _parse_access_logs(self, batch_number: str, access_logs: List[Tuple]) -> None:
        logger.info(f"access logs batch #{batch_number}")
        for query_id, start_time, accessed_objects in access_logs:
            self._parse_access_log(start_time, accessed_objects)

    def _parse_access_log(self, start_time: datetime, accessed_objects: str) -> None:
        try:
            objects = from_json(List[AccessedObject], accessed_objects)
            for obj in objects:
                table_name = obj.objectName.lower()
                parts = table_name.split(".")
                db, schema, table = parts[0], parts[1], parts[2]
                if not include_table(db, schema, table, self.filter):
                    logger.debug(f"Ignore {table_name} due to filter config")
                    continue

                if table_name not in self._datasets:
                    self._datasets[table_name] = SnowflakeUsageExtractor._init_dataset(
                        self.account, table_name
                    )

                columns = [column.columnName.lower() for column in obj.columns]

                self._update_table_and_columns_usage(table_name, columns, start_time)
        except Exception as e:
            logger.error(f"access log error, objects: {accessed_objects}")
            logger.exception(e)

    def _update_table_and_columns_usage(
        self, table_name: str, columns: List[str], start_time: datetime
    ) -> None:
        utc_now = datetime.now().replace(tzinfo=timezone.utc)
        if start_time > utc_now - timedelta(1):
            self._datasets[table_name].usage.query_counts.last24_hours.count += 1

        if start_time > utc_now - timedelta(7):
            self._datasets[table_name].usage.query_counts.last7_days.count += 1

        if start_time > utc_now - timedelta(30):
            self._datasets[table_name].usage.query_counts.last30_days.count += 1

        if start_time > utc_now - timedelta(90):
            self._datasets[table_name].usage.query_counts.last90_days.count += 1

        if start_time > utc_now - timedelta(365):
            self._datasets[table_name].usage.query_counts.last365_days.count += 1

        for column_name in columns:
            if start_time > utc_now - timedelta(1):
                self._update_field_query_count(
                    self._datasets[table_name].usage.field_query_counts.last24_hours,
                    column_name,
                )

            if start_time > utc_now - timedelta(7):
                self._update_field_query_count(
                    self._datasets[table_name].usage.field_query_counts.last7_days,
                    column_name,
                )

            if start_time > utc_now - timedelta(30):
                self._update_field_query_count(
                    self._datasets[table_name].usage.field_query_counts.last30_days,
                    column_name,
                )

            if start_time > utc_now - timedelta(90):
                self._update_field_query_count(
                    self._datasets[table_name].usage.field_query_counts.last90_days,
                    column_name,
                )

            if start_time > utc_now - timedelta(365):
                self._update_field_query_count(
                    self._datasets[table_name].usage.field_query_counts.last365_days,
                    column_name,
                )

    @staticmethod
    def _update_field_query_count(query_counts: List[FieldQueryCount], column: str):
        item = next((x for x in query_counts if x.field == column), None)
        if item:
            item.count += 1
        else:
            query_counts.append(FieldQueryCount(field=column, count=1.0))

    def _calculate_statistics(self) -> None:
        """Calculate statistics for the extracted usage info"""
        datasets = self._datasets.values()

        self.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last24_hours
        )

        self.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last7_days
        )

        self.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last30_days
        )

        self.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last90_days
        )

        self.calculate_table_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last365_days
        )

        for dataset in datasets:
            self.calculate_column_percentile(
                dataset.usage.field_query_counts.last24_hours
            )
            self.calculate_column_percentile(
                dataset.usage.field_query_counts.last7_days
            )
            self.calculate_column_percentile(
                dataset.usage.field_query_counts.last30_days
            )
            self.calculate_column_percentile(
                dataset.usage.field_query_counts.last90_days
            )
            self.calculate_column_percentile(
                dataset.usage.field_query_counts.last365_days
            )

    @staticmethod
    def calculate_table_percentile(
        datasets: Collection[Dataset], get_query_count: Callable[[Dataset], QueryCount]
    ) -> None:
        counts = [get_query_count(dataset).count for dataset in datasets]
        counts.sort(reverse=True)

        for dataset in datasets:
            query_count = get_query_count(dataset)
            query_count.percentile = 1.0 - counts.index(query_count.count) / len(
                datasets
            )

    @staticmethod
    def calculate_column_percentile(columns: List[FieldQueryCount]) -> None:
        counts = [column.count for column in columns]
        counts.sort(reverse=True)

        for column in columns:
            column.percentile = 1.0 - counts.index(column.count) / len(columns)

    @staticmethod
    def _init_dataset(account: str, full_name: str) -> Dataset:
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=full_name, account=account, platform=DataPlatform.SNOWFLAKE
        )

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
