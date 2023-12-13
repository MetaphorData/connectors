from typing import Collection

from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.postgresql.usage.config import PostgreSQLUsageRunConfig

logger = get_logger()


USAGE_SQL = """
SELECT schemaname, relname, seq_scan FROM pg_stat_user_tables
"""


class PostgreSQLUsageExtractor(PostgreSQLExtractor):
    """PostgreSQL usage metadata extractor"""

    _description = "PostgreSQL usage statistics crawler"

    @staticmethod
    def from_config_file(config_file: str) -> "PostgreSQLUsageExtractor":
        return PostgreSQLUsageExtractor(
            PostgreSQLUsageRunConfig.from_yaml_file(config_file)
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching usage metadata from PostgreSQL host {self._host}")

        databases = (
            await self._fetch_databases()
            if self._filter.includes is None
            else list(self._filter.includes.keys())
        )

        datasets = []

        for db in databases:
            conn = await self._connect_database(db)
            try:
                results = await conn.fetch(USAGE_SQL)
                for row in results:
                    schema = row["schemaname"]
                    table_name = row["relname"]
                    read_count = row["seq_scan"]
                    normalized_name = dataset_normalized_name(db, schema, table_name)

                    if not self._filter.include_table(db, schema, table_name):
                        logger.info(f"Ignore {normalized_name} due to filter config")
                        continue

                    dataset = UsageUtil.init_dataset(
                        normalized_name, DataPlatform.POSTGRESQL
                    )

                    # don't have exact time of query, so set all time window to be same query count
                    dataset.usage.query_counts.last24_hours.count = float(read_count)
                    dataset.usage.query_counts.last7_days.count = float(read_count)
                    dataset.usage.query_counts.last30_days.count = float(read_count)
                    dataset.usage.query_counts.last90_days.count = float(read_count)
                    dataset.usage.query_counts.last365_days.count = float(read_count)
                    datasets.append(dataset)
            finally:
                await conn.close()

        UsageUtil.calculate_statistics(datasets)
        return datasets
