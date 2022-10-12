from typing import Collection

from metaphor.common.entity_id import dataset_fullname
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.config import RedshiftRunConfig

logger = get_logger(__name__)


class RedshiftExtractor(PostgreSQLExtractor):
    """Redshift metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftExtractor":
        return RedshiftExtractor(RedshiftRunConfig.from_yaml_file(config_file))

    def __init__(self, config: RedshiftRunConfig):
        super().__init__(
            config,
            "Redshift metadata crawler",
            Platform.REDSHIFT,
            DataPlatform.REDSHIFT,
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from redshift host {self._host}")

        databases = (
            await self._fetch_databases()
            if self._filter.includes is None
            else list(self._filter.includes.keys())
        )

        for db in databases:
            conn = await self._connect_database(db)
            try:
                await self._fetch_tables(conn, db, True)
                await self._fetch_columns(conn, db, True)
                await self._fetch_redshift_table_stats(conn, db)
            except Exception as ex:
                logger.exception(ex)
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
