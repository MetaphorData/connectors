from typing import Collection, List

from metaphor.common.constants import BYTES_PER_MEGABYTES
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.query_history import chunk_query_logs
from metaphor.common.tag_matcher import tag_datasets
from metaphor.common.utils import md5_digest, start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform, QueriedDataset, QueryLog
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.access_event import AccessEvent
from metaphor.redshift.config import RedshiftRunConfig

logger = get_logger()


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
        self._tag_matchers = config.tag_matchers
        self._query_log_lookback_days = config.query_log.lookback_days
        self._query_log_excluded_usernames = config.query_log.excluded_usernames

        self._logs: List[QueryLog] = []

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
                await self._fetch_query_logs(conn)
            except Exception as ex:
                logger.exception(ex)
            finally:
                await conn.close()

        datasets = list(self._datasets.values())
        tag_datasets(datasets, self._tag_matchers)

        entities: List[ENTITY_TYPES] = []
        entities.extend(datasets)
        entities.extend(chunk_query_logs(self._logs))
        return entities

    async def _fetch_redshift_table_stats(self, conn, catalog: str) -> None:
        results = await conn.fetch(
            """
            SELECT "schema", "table", size, tbl_rows
            FROM pg_catalog.svv_table_info;
            """,
        )

        for result in results:
            normalized_name = dataset_normalized_name(
                catalog, result["schema"], result["table"]
            )
            if normalized_name not in self._datasets:
                logger.warning(f"table {normalized_name} not found")
                continue

            dataset = self._datasets[normalized_name]
            assert dataset.statistics is not None

            statistics = to_dataset_statistics(
                result["tbl_rows"], result["size"] * BYTES_PER_MEGABYTES
            )
            dataset.statistics.record_count = statistics.record_count
            dataset.statistics.data_size = statistics.data_size
            dataset.statistics.data_size_bytes = statistics.data_size_bytes

    async def _fetch_query_logs(self, conn) -> None:
        logger.info("Fetching query logs")

        start_date = start_of_day(self._query_log_lookback_days)
        end_date = start_of_day()

        async for record in AccessEvent.fetch_access_event(conn, start_date, end_date):
            self._process_record(record)

    def _process_record(self, access_event: AccessEvent):
        if not self._filter.include_table(
            access_event.database, access_event.schema, access_event.table
        ):
            return

        if access_event.usename in self._query_log_excluded_usernames:
            return

        dataset = QueriedDataset(
            database=access_event.database,
            schema=access_event.schema,
            table=access_event.table,
        )

        query_log = QueryLog(
            id=f"{DataPlatform.REDSHIFT.name}:{access_event.query}",
            query_id=str(access_event.query),
            platform=DataPlatform.REDSHIFT,
            start_time=access_event.starttime,
            duration=float(
                (access_event.endtime - access_event.starttime).total_seconds()
            ),
            user_id=access_event.usename,
            rows_read=float(access_event.rows),
            bytes_read=float(access_event.bytes),
            sources=[dataset],
            sql=access_event.querytxt,
            sql_hash=md5_digest(access_event.querytxt.encode("utf-8")),
        )

        self._logs.append(query_log)
