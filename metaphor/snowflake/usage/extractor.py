import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch
from typing import Callable, Collection, Dict, List, Set

from serde import deserialize

from metaphor.common.event_util import EventUtil

try:
    import snowflake.connector
    import sql_metadata
except ImportError:
    print("Please install metaphor[snowflake] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    AspectType,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUsage,
    EntityType,
    MetadataChangeEvent,
    QueryCount,
    QueryCounts,
)

from metaphor.common.extractor import BaseExtractor, RunConfig

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class SnowflakeUsageRunConfig(RunConfig):
    account: str
    user: str
    password: str

    # The number of days in history to retrieve query log
    lookback_days: int = 30

    # Query filter to only include logs against database in included_databases
    included_databases: Set[str] = field(default_factory=lambda: set())

    # Query filter to exclude logs whose user name is in excluded_usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # Post filter to only include table names that matches the pattern with shell-style wildcards
    included_table_names: Set[str] = field(default_factory=lambda: {"*"})

    # Post filter to exclude table names that matches the pattern with shell-style wildcards
    excluded_table_names: Set[str] = field(default_factory=lambda: set())


class SnowflakeUsageExtractor(BaseExtractor):
    """Snowflake usage metadata extractor"""

    @staticmethod
    def config_class():
        return SnowflakeUsageRunConfig

    def __init__(self):
        self._datasets: Dict[str, Dataset] = {}
        self.total_queries_count = 0
        self.error_count = 0
        self.included_table_names = []
        self.excluded_table_names = []

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeUsageExtractor.config_class())

        logger.info(f"Fetching usage info from Snowflake account {config.account}")
        ctx = snowflake.connector.connect(
            account=config.account, user=config.user, password=config.password
        )

        self.included_table_names = [
            name.lower() for name in config.included_table_names
        ]
        self.excluded_table_names = [
            name.lower() for name in config.excluded_table_names
        ]

        included_databases_clause = (
            f"and DATABASE_NAME IN ({','.join(['%s'] * len(config.included_databases))})"
            if config.included_databases
            else ""
        )
        excluded_usernames_clause = (
            f"and USER_NAME NOT IN ({','.join(['%s'] * len(config.excluded_usernames))})"
            if config.excluded_usernames
            else ""
        )

        start_date = datetime.utcnow().date() - timedelta(config.lookback_days)
        query_id = "0"  # query id initial value for batch filtering
        batch_size = 1000

        with ctx:
            cursor = ctx.cursor()
            cursor.execute("USE ROLE accountadmin")

            while True:
                cursor.execute(
                    "SELECT QUERY_ID, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, START_TIME "
                    "FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY "
                    "WHERE SCHEMA_NAME != 'NULL' and EXECUTION_STATUS = 'SUCCESS' "
                    "  and START_TIME > %s and QUERY_ID > %s "
                    f"  {included_databases_clause} {excluded_usernames_clause} "
                    "ORDER BY QUERY_ID "
                    "LIMIT %s ",
                    (
                        start_date,
                        query_id,
                        *config.included_databases,
                        *config.excluded_usernames,
                        batch_size,
                    ),
                )
                queries = [row for row in cursor]
                logger.debug(f"Queries: {queries}")

                for query in queries:
                    self._parse_query_tables(query[1], query[2], query[3], query[4])

                self.total_queries_count += len(queries)
                logger.info(
                    f"total queries: {self.total_queries_count}, errors {self.error_count}"
                )

                if len(queries) < batch_size:
                    break
                # set last query id for filtering in next batch
                query_id = queries[-1][0]

        # calculate statistics based on the counts
        self._calculate_statistics()

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _parse_query_tables(
        self, query: str, db: str, schema: str, start_time: datetime
    ) -> None:
        tables = []
        try:
            tables = sql_metadata.Parser(query).tables
        except Exception:
            self.error_count += 1
        logger.debug(f"tables: {tables}")

        for table in tables:
            fullname = SnowflakeUsageExtractor._built_table_fullname(table, db, schema)
            if not self._filter_table_names(fullname):
                continue

            if fullname not in self._datasets:
                self._datasets[fullname] = SnowflakeUsageExtractor._init_dataset(
                    fullname
                )

            utc_now = datetime.now().replace(tzinfo=timezone.utc)
            if start_time > utc_now - timedelta(1):
                self._datasets[fullname].usage.query_counts.last24_hours.count += 1

            if start_time > utc_now - timedelta(7):
                self._datasets[fullname].usage.query_counts.last7_days.count += 1

            if start_time > utc_now - timedelta(30):
                self._datasets[fullname].usage.query_counts.last30_days.count += 1

            if start_time > utc_now - timedelta(90):
                self._datasets[fullname].usage.query_counts.last90_days.count += 1

            if start_time > utc_now - timedelta(365):
                self._datasets[fullname].usage.query_counts.last365_days.count += 1

    def _filter_table_names(self, table_fullname: str) -> bool:
        """Filter table names based on included/excluded table names in config"""

        included = False
        for pattern in self.included_table_names:
            if fnmatch(table_fullname, pattern):
                included = True
                break

        if not included:
            return False

        for pattern in self.excluded_table_names:
            if fnmatch(table_fullname, pattern):
                return False

        return True

    @staticmethod
    def _built_table_fullname(table: str, db: str, schema: str) -> str:
        dots = table.count(".")
        if dots == 0:
            return f"{db}.{schema}.{table}".lower()
        elif dots == 1:
            return f"{db}.{table}".lower()
        else:  # should have at most two dots in SQL table name
            return table.lower()

    def _calculate_statistics(self) -> None:
        """Calculate statistics for the extracted Dataset usages"""
        datasets = self._datasets.values()

        self.calculate_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last24_hours
        )

        self.calculate_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last7_days
        )

        self.calculate_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last30_days
        )

        self.calculate_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last90_days
        )

        self.calculate_percentile(
            datasets, lambda dataset: dataset.usage.query_counts.last365_days
        )

    @staticmethod
    def calculate_percentile(
        datasets: Collection[Dataset], get_query_count: Callable[[Dataset], QueryCount]
    ) -> None:
        counts = [get_query_count(dataset).count for dataset in datasets]
        counts.sort()

        for dataset in datasets:
            query_count = get_query_count(dataset)
            query_count.percentile = counts.index(query_count.count) / len(datasets)

    @staticmethod
    def _init_dataset(full_name: str) -> Dataset:
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=full_name, platform=DataPlatform.SNOWFLAKE
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

        return dataset
