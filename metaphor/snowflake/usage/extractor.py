import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set

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

    # Exclude queries whose user name is in the excluded_usernames
    excluded_databases: Set[str] = field(default_factory=lambda: set())

    # Exclude queries whose user name is in the excluded_usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())


class SnowflakeUsageExtractor(BaseExtractor):
    """Snowflake usage metadata extractor"""

    @staticmethod
    def config_class():
        return SnowflakeUsageRunConfig

    def __init__(self):
        self._datasets: Dict[str, Dataset] = {}
        self.total_queries_count = 0
        self.error_count = 0

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeUsageExtractor.config_class())

        logger.info(f"Fetching usage info from Snowflake account {config.account}")
        ctx = snowflake.connector.connect(
            account=config.account, user=config.user, password=config.password
        )

        excluded_databases = ",".join(config.excluded_databases)
        excluded_usernames = ",".join(config.excluded_usernames)
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
                    "WHERE SCHEMA_NAME != 'NULL' and START_TIME > %s and QUERY_ID > %s "
                    "  and DATABASE_NAME NOT IN (%s) and USER_NAME NOT IN (%s) "
                    "ORDER BY QUERY_ID "
                    "LIMIT %s ",
                    (
                        start_date,
                        query_id,
                        excluded_databases,
                        excluded_usernames,
                        batch_size,
                    ),
                )
                queries = [row for row in cursor]
                logger.debug(f"Queries: {queries}")

                for query in queries:
                    self._parse_query_tables(query[1], query[2], query[3], query[4])

                self.total_queries_count += len(queries)
                query_id = queries[-1][0]
                logger.info(
                    f"total queries: {self.total_queries_count}, errors {self.error_count}, last query id: {query_id}"
                )

                if len(queries) < batch_size:
                    break

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _parse_query_tables(
        self, query: str, db: str, schema: str, start_time: datetime
    ):
        tables = []
        try:
            tables = sql_metadata.Parser(query).tables
        except Exception:
            self.error_count += 1
        logger.debug(f"tables: {tables}")

        for table in tables:
            fullname = SnowflakeUsageExtractor._built_table_fullname(table, db, schema)
            if fullname not in self._datasets:
                self._datasets[fullname] = SnowflakeUsageExtractor._init_dataset(
                    fullname
                )

            utc_now = datetime.now().replace(tzinfo=timezone.utc)
            if start_time > utc_now - timedelta(1):
                self._datasets[fullname].usage.query_count.last24_hours += 1

            if start_time > utc_now - timedelta(7):
                self._datasets[fullname].usage.query_count.last7_days += 1

            if start_time > utc_now - timedelta(30):
                self._datasets[fullname].usage.query_count.last30_days += 1

            if start_time > utc_now - timedelta(90):
                self._datasets[fullname].usage.query_count.last90_days += 1

            if start_time > utc_now - timedelta(365):
                self._datasets[fullname].usage.query_count.last365_days += 1

    @staticmethod
    def _built_table_fullname(table: str, db: str, schema: str) -> str:
        dots = table.count(".")
        if dots == 0:
            return f"{db}.{schema}.{table}".lower()
        elif dots == 1:
            return f"{db}.{table}".lower()
        else:  # should have at most two dots in SQL table name
            return table.lower()

    @staticmethod
    def _init_dataset(full_name: str) -> Dataset:
        dataset = Dataset()
        dataset.entity_type = EntityType.DATASET
        dataset.logical_id = DatasetLogicalID(
            name=full_name, platform=DataPlatform.SNOWFLAKE
        )

        dataset.usage = DatasetUsage(aspect_type=AspectType.DATASET_USAGE)
        dataset.usage.query_count = QueryCount(
            # quicktype bug: if use integer 0, "to_dict" will throw AssertionError as it expect float
            last24_hours=0.0,
            last7_days=0.0,
            last30_days=0.0,
            last90_days=0.0,
            last365_days=0.0,
        )

        return dataset
