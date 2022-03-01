from typing import Collection

from metaphor.models.metadata_change_event import DataPlatform

from metaphor.common.entity_id import dataset_fullname
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.usage_util import UsageUtil
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.postgresql.usage.config import PostgreSQLUsageRunConfig

logger = get_logger(__name__)


USAGE_SQL = """
SELECT schemaname, relname, seq_scan FROM pg_stat_user_tables
"""


class PostgreSQLUsageExtractor(PostgreSQLExtractor):
    """PostgreSQL usage metadata extractor"""

    @staticmethod
    def config_class():
        return PostgreSQLUsageRunConfig

    async def extract(
        self, config: PostgreSQLUsageRunConfig
    ) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching usage metadata from PostgreSQL host {config.host}")

        dataset_filter = config.filter.normalize()

        databases = (
            await self._fetch_databases(config)
            if dataset_filter.includes is None
            else list(dataset_filter.includes.keys())
        )

        datasets = []

        for db in databases:
            conn = await self._connect_database(config, db)
            try:
                results = await conn.fetch(USAGE_SQL)
                for row in results:
                    schema = row["schemaname"]
                    table_name = row["relname"]
                    read_count = row["seq_scan"]
                    full_name = dataset_fullname(db, schema, table_name)

                    if not dataset_filter.include_table(db, schema, table_name):
                        logger.info(f"Ignore {full_name} due to filter config")
                        continue

                    dataset = UsageUtil.init_dataset(
                        None, full_name, DataPlatform.POSTGRESQL
                    )
                    dataset.usage.query_counts.last30_days.count = float(read_count)
                    dataset.usage.query_counts.last90_days.count = float(read_count)
                    dataset.usage.query_counts.last365_days.count = float(read_count)
                    datasets.append(dataset)
            finally:
                await conn.close()

        UsageUtil.calculate_statistics(datasets)
        return datasets
