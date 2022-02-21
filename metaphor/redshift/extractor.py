from typing import Collection

from metaphor.models.metadata_change_event import DataPlatform

from metaphor.common.entity_id import dataset_fullname
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.config import RedshiftRunConfig

logger = get_logger(__name__)


class RedshiftExtractor(PostgreSQLExtractor):
    """Redshift metadata extractor"""

    @staticmethod
    def config_class():
        return RedshiftRunConfig

    def __init__(self):
        super().__init__()
        self._platform = DataPlatform.REDSHIFT

    async def extract(self, config: RedshiftRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching metadata from redshift host {config.host}")

        filter = config.filter.normalize()

        databases = (
            await self._fetch_databases(config)
            if filter.includes is None
            else list(filter.includes.keys())
        )

        for db in databases:
            conn = await self._connect_database(config, db)
            try:
                await self._fetch_tables(conn, db, filter)
                await self._fetch_columns(conn, db, filter)
                await self._fetch_redshift_table_stats(conn, db)
            finally:
                await conn.close()

        return self._datasets.values()

    async def _fetch_redshift_table_stats(self, conn, catalog: str) -> None:
        results = await conn.fetch(
            """
            SELECT "schema", "table", size, tbl_rows
            FROM pg_catalog.svv_table_info;
            """,
        )

        for result in results:
            full_name = dataset_fullname(catalog, result["schema"], result["table"])
            if full_name not in self._datasets:
                logger.warning(f"table {full_name} not found")
                continue

            dataset = self._datasets[full_name]
            assert dataset.statistics is not None

            dataset.statistics.record_count = (
                float(result["tbl_rows"]) if result["tbl_rows"] is not None else None
            )
            dataset.statistics.data_size = (
                float(result["size"]) if result["size"] is not None else None
            )
