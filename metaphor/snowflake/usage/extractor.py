import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Collection, Dict, List, Set, Tuple

from sql_metadata import Parser

from metaphor.common.entity_id import EntityId
from metaphor.common.event_util import EventUtil
from metaphor.common.logging import get_logger
from metaphor.snowflake.auth import connect
from metaphor.snowflake.usage.config import SnowflakeUsageRunConfig
from metaphor.snowflake.utils import QueryWithParam, async_execute, include_table

try:
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
    FieldQueryCount,
    FieldQueryCounts,
    MetadataChangeEvent,
    QueryCount,
    QueryCounts,
    TableColumnsUsage,
    TableJoin,
    TableJoins,
    TableJoinScenario,
)

from metaphor.common.extractor import BaseExtractor

logger = get_logger(__name__)

# disable logging from sql_metadata
logging.getLogger("Parser").setLevel(logging.CRITICAL)


DEFAULT_EXCLUDED_DATABASES = ["SNOWFLAKE"]


class SnowflakeUsageExtractor(BaseExtractor):
    """Snowflake usage metadata extractor"""

    @staticmethod
    def config_class():
        return SnowflakeUsageRunConfig

    def __init__(self):
        self.account = None
        self.max_concurrency = None
        self._datasets: Dict[str, Dataset] = {}
        self.total_queries_count = 0
        self.error_count = 0

    async def extract(
        self, config: SnowflakeUsageRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, SnowflakeUsageExtractor.config_class())

        logger.info("Fetching usage info from Snowflake")

        self.account = config.account
        self.max_concurrency = config.max_concurrency
        self.filter = config.filter.normalize()

        self.filter.excludes = (
            DEFAULT_EXCLUDED_DATABASES
            if self.filter.excludes is None
            else self.filter.excludes + DEFAULT_EXCLUDED_DATABASES
        )

        # Database names must be capitalized for the IN clause to work
        included_databases = (
            [d.upper() for d in list(config.filter.includes.keys())]
            if config.filter.includes
            else []
        )

        included_databases_clause = (
            f"and DATABASE_NAME IN ({','.join(['%s'] * len(included_databases))})"
            if len(included_databases) > 0
            else ""
        )

        excluded_usernames_clause = (
            f"and USER_NAME NOT IN ({','.join(['%s'] * len(config.excluded_usernames))})"
            if len(config.excluded_usernames) > 0
            else ""
        )

        start_date = datetime.utcnow().date() - timedelta(config.lookback_days)
        batch_size = 1000

        conn = connect(config)

        with conn:
            cursor = conn.cursor()

            cursor.execute(
                f"""
                SELECT COUNT(1), MIN(QUERY_ID)
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE SCHEMA_NAME != 'NULL' and EXECUTION_STATUS = 'SUCCESS' and START_TIME > %s
                  {included_databases_clause} {excluded_usernames_clause}
                """,
                (
                    start_date,
                    *included_databases,
                    *config.excluded_usernames,
                ),
            )
            count, min_query_id = cursor.fetchone()
            batches = count // batch_size + 1
            logger.info(f"Total {count} queries, dividing into {batches} batches")

            queries = {
                x: QueryWithParam(
                    f"""
                    SELECT QUERY_ID, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, START_TIME
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE SCHEMA_NAME != 'NULL' and EXECUTION_STATUS = 'SUCCESS' and START_TIME > %s
                      {included_databases_clause} {excluded_usernames_clause}
                    ORDER BY QUERY_ID
                    LIMIT {batch_size} OFFSET %s
                    """,
                    (
                        start_date,
                        *included_databases,
                        *config.excluded_usernames,
                        x * batch_size,
                    ),
                )
                for x in range(batches)
            }
            async_execute(
                conn,
                queries,
                "fetch_queries",
                self.max_concurrency,
                self._parse_query_logs,
            )

        # calculate statistics based on the counts
        self._calculate_statistics()

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    def _parse_query_logs(self, batch_number: str, queries: List[Tuple]):
        logger.info(f"query logs batch #{batch_number}")
        for query_id, query, db, schema, start_time in queries:
            self._parse_query_log(query, db, schema, start_time)

    def _parse_query_log(
        self, query: str, db: str, schema: str, start_time: datetime
    ) -> None:
        try:
            parser = sql_metadata.Parser(query)
            tables = self._parse_tables(parser, db, schema, start_time)
            self._parse_columns(parser, db, schema, tables, start_time)
            self._parse_joins(parser, db, schema, start_time)
        except (KeyError, ValueError) as e:
            self.error_count += 1
            logger.debug(f"{e} {query}")
        except Exception as e:
            self.error_count += 1
            logger.error(f"parser error {e}: {query}")

    def _parse_tables(
        self, parser: Parser, db: str, schema: str, start_time: datetime
    ) -> List[str]:
        logger.debug(f"tables: {parser.tables}")

        table_fullnames = []
        for table in parser.tables:
            fullname = SnowflakeUsageExtractor._built_table_fullname(table, db, schema)
            if not include_table(db, schema, table, self.filter):
                logger.info(f"Ignore {fullname} due to filter config")
                continue

            table_fullnames.append(fullname)

            if fullname not in self._datasets:
                self._datasets[fullname] = SnowflakeUsageExtractor._init_dataset(
                    self.account, fullname
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

        return table_fullnames

    def _parse_columns(
        self,
        parser: Parser,
        db: str,
        schema: str,
        tables: List[str],
        start_time: datetime,
    ) -> None:
        logger.debug(f"columns: {parser.columns}")
        for column in parser.columns:
            table_name, column_name = SnowflakeUsageExtractor.built_column_fullname(
                column, db, schema, tables
            )
            if column_name == "*" or table_name not in self._datasets:
                logger.debug(f"table {table_name} column {column_name} passed")
                continue

            utc_now = datetime.now().replace(tzinfo=timezone.utc)
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

    @dataclass
    class _ColumnsUsage:
        join: Set[str] = field(default_factory=lambda: set())
        filter: Set[str] = field(default_factory=lambda: set())

    def _parse_joins(
        self,
        parser: Parser,
        db: str,
        schema: str,
        start_time: datetime,
    ) -> None:
        logger.debug(f"column_dict: {parser.columns_dict}")

        if parser.columns_dict is None or "join" not in parser.columns_dict:
            logger.debug(f"parser query {parser.query}")
            return

        usages: Dict[str, SnowflakeUsageExtractor._ColumnsUsage] = defaultdict(
            lambda: SnowflakeUsageExtractor._ColumnsUsage()
        )  # table_name to ColumnUsage map

        for join_column in parser.columns_dict["join"]:
            table, column = self.built_column_fullname(join_column, db, schema, [])
            usages[table].join.add(column)

        for filter_column in parser.columns_dict.get("where", []):
            table, column = self.built_column_fullname(filter_column, db, schema, [])
            usages[table].filter.add(column)

        # update table joins usages for each dataset
        for table_name in usages:
            if table_name not in self._datasets:
                logger.debug(f"table {table_name} passed")
                continue

            table_joins = self._datasets[table_name].usage.table_joins
            usage = usages[table_name]
            joined_datasets = set(
                [
                    str(
                        EntityId(
                            EntityType.DATASET,
                            DatasetLogicalID(
                                name=name,
                                platform=DataPlatform.SNOWFLAKE,
                                account=self.account,
                            ),
                        )
                    )
                    for name in usages.keys()
                    if name != table_name
                ]
            )  # convert joined tables into DatasetId

            utc_now = datetime.now().replace(tzinfo=timezone.utc)
            if start_time > utc_now - timedelta(1):
                self._update_table_join(
                    table_joins.last24_hours, joined_datasets, usage
                )

            if start_time > utc_now - timedelta(7):
                self._update_table_join(table_joins.last7_days, joined_datasets, usage)

            if start_time > utc_now - timedelta(30):
                self._update_table_join(table_joins.last30_days, joined_datasets, usage)

            if start_time > utc_now - timedelta(90):
                self._update_table_join(table_joins.last90_days, joined_datasets, usage)

            if start_time > utc_now - timedelta(365):
                self._update_table_join(
                    table_joins.last365_days, joined_datasets, usage
                )

    @staticmethod
    def _update_table_join(
        table_join: TableJoin, joined_datasets: Set[str], usage: _ColumnsUsage
    ) -> None:
        table_join.total_join_count += 1

        # find the join scenario, or create new scenario
        scenario = None
        for join_scenario in table_join.scenarios:
            if set(join_scenario.datasets) == joined_datasets:
                scenario = join_scenario
                break

        if scenario is None:
            scenario = TableJoinScenario(
                datasets=list(joined_datasets),
                count=0.0,
                joining_columns=[],
                filtering_columns=[],
            )
            table_join.scenarios.append(scenario)

        scenario.count += 1

        # update joiningColumns
        SnowflakeUsageExtractor._update_table_join_columns_usage(
            scenario.joining_columns, usage.join
        )

        # update filteringColumns
        if len(usage.filter) > 0:
            SnowflakeUsageExtractor._update_table_join_columns_usage(
                scenario.filtering_columns, usage.filter
            )

    @staticmethod
    def _update_table_join_columns_usage(
        column_usages: List[TableColumnsUsage], columns: Set[str]
    ) -> None:
        columns_usage = None
        for usage in column_usages:
            if set(usage.columns) == columns:
                columns_usage = usage
                break

        if columns_usage is None:
            columns_usage = TableColumnsUsage(columns=list(columns), count=0.0)
            column_usages.append(columns_usage)

        columns_usage.count += 1

    @staticmethod
    def _update_field_query_count(query_counts: List[FieldQueryCount], column: str):
        item = next((x for x in query_counts if x.field == column), None)
        if item:
            item.count += 1
        else:
            query_counts.append(FieldQueryCount(field=column, count=1.0))

    @staticmethod
    def _built_table_fullname(table: str, db: str, schema: str) -> str:
        """built table fullname with <DB>.<SCHEMA>.<TABLE>, in lowercase"""
        dots = table.count(".")
        if dots == 0:
            return f"{db}.{schema}.{table}".lower()
        elif dots == 1:
            return f"{db}.{table}".lower()
        else:  # should have at most two dots in SQL table name
            return table.lower()

    @staticmethod
    def built_column_fullname(
        column: str, db: str, schema: str, tables: List[str]
    ) -> Tuple[str, str]:
        """built column fullname with (<DB>.<SCHEMA>.<TABLE>, <COLUMN>), in lowercase"""
        table, _, column_name = column.rpartition(".")
        dots = column.count(".")
        if dots == 0:  # contains only column name
            if len(tables) != 1:
                raise ValueError(f"Query should have only 1 table, got {tables}")
            return tables[0], column_name.lower()
        elif dots == 1:  # should be at least "table.column"
            return f"{db}.{schema}.{table}".lower(), column_name.lower()
        elif dots == 2:
            return f"{db}.{table}".lower(), column_name.lower()
        else:  # should have at most three dots in SQL column name: db.schema.table.column
            return table.lower(), column_name.lower()

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
        dataset.usage.table_joins = TableJoins(
            last24_hours=TableJoin(total_join_count=0.0, scenarios=[]),
            last7_days=TableJoin(total_join_count=0.0, scenarios=[]),
            last30_days=TableJoin(total_join_count=0.0, scenarios=[]),
            last90_days=TableJoin(total_join_count=0.0, scenarios=[]),
            last365_days=TableJoin(total_join_count=0.0, scenarios=[]),
        )

        return dataset
