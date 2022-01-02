from typing import List

from metaphor.models.metadata_change_event import DataPlatform, MetadataChangeEvent

from metaphor.common.event_util import EventUtil
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

    async def extract(self, config: RedshiftRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching metadata from redshift host {config.host}")

        databases = await self._fetch_databases(config)

        for db in databases:
            conn = await self._connect_database(config, db)
            try:
                await self._fetch_tables(conn)
                await self._fetch_columns(conn, db)
                await self._fetch_redshift_table_stats(conn, db)
            finally:
                await conn.close()

        return [EventUtil.build_dataset_event(d) for d in self._datasets.values()]

    async def _fetch_redshift_table_stats(self, conn, catalog: str) -> None:
        results = await conn.fetch(
            """
            SELECT "schema", "table", size, tbl_rows
            FROM svv_table_info;
            """,
        )

        for result in results:
            full_name = self._dataset_name(catalog, result["schema"], result["table"])
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
