import asyncio
import datetime
from typing import Collection, Iterator, List, Set

from metaphor.common.constants import BYTES_PER_MEGABYTES
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.sql.query_log import PartialQueryLog, process_and_init_query_log
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.tag_matcher import tag_datasets
from metaphor.common.utils import start_of_day
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform, QueriedDataset, QueryLog
from metaphor.postgresql.extractor import BasePostgreSQLExtractor
from metaphor.redshift.access_event import AccessEvent
from metaphor.redshift.config import RedshiftRunConfig
from metaphor.redshift.utils import exclude_system_databases

logger = get_logger()


class RedshiftExtractor(BasePostgreSQLExtractor):
    """Redshift metadata extractor"""

    _description = "Redshift metadata crawler"
    _platform = Platform.REDSHIFT
    _dataset_platform = DataPlatform.REDSHIFT

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftExtractor":
        return RedshiftExtractor(RedshiftRunConfig.from_yaml_file(config_file))

    def __init__(self, config: RedshiftRunConfig):
        super().__init__(config)
        self._tag_matchers = config.tag_matchers
        self._query_log_lookback_days = config.query_log.lookback_days

        # Exclude metaphor user
        self._query_log_excluded_usernames = config.query_log.excluded_usernames
        self._query_log_excluded_usernames.add(config.user)

        self._filter = exclude_system_databases(self._filter)

        self._logs: List[QueryLog] = []
        self._included_databases: Set[str] = set()

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from redshift host {self._host}")

        databases = (
            await self._fetch_databases()
            if self._filter.includes is None
            else list(self._filter.includes.keys())
        )

        for db in databases:
            if not self._filter.include_database(db):
                logger.info(f"Skipping database {db}")
                continue
            self._included_databases.add(db)

            conn = await self._connect_database(db)
            try:
                await self._fetch_tables(conn, db, True)
                await self._fetch_columns(conn, db, True)
                await self._fetch_redshift_table_stats(conn, db)
            except Exception as ex:
                logger.exception(ex)
            finally:
                await conn.close()

        datasets = list(self._datasets.values())
        tag_datasets(datasets, self._tag_matchers)

        entities: List[ENTITY_TYPES] = []
        entities.extend(datasets)
        return entities

    def collect_query_logs(self) -> Iterator[QueryLog]:
        """
        Prerequisite: `extract` must be called before this method is called.
        """
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        query_log_count = 0
        for db in self._included_databases:
            conn = loop.run_until_complete(self._connect_database(db))
            aiter = self._fetch_query_logs(conn).__aiter__()

            async def get_next():
                try:
                    query_log = await aiter.__anext__()
                    return query_log
                except StopAsyncIteration:
                    return None

            while True:
                query_log = loop.run_until_complete(get_next())
                if query_log:
                    yield query_log
                    query_log_count += 1
                else:
                    break
        logger.info(f"Wrote {query_log_count} QueryLog")

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
            dataset.statistics.data_size_bytes = statistics.data_size_bytes

    async def _fetch_query_logs(self, conn):
        logger.info("Fetching query logs")

        start_date = start_of_day(self._query_log_lookback_days)
        end_date = datetime.datetime.now(tz=datetime.timezone.utc)

        async for record in AccessEvent.fetch_access_event(conn, start_date, end_date):
            query_log = self._process_record(record)
            if query_log:
                yield query_log

    def _is_related_query_log(self, queried_datasets: List[QueriedDataset]) -> bool:
        for dataset in queried_datasets:
            table_name = dataset_normalized_name(
                db=dataset.database, schema=dataset.schema, table=dataset.table
            )

            if table_name in self._datasets:
                return True
        return False

    def _process_record(self, access_event: AccessEvent):
        if access_event.usename in self._query_log_excluded_usernames:
            return

        tll = extract_table_level_lineage(
            sql=access_event.querytxt,
            platform=DataPlatform.REDSHIFT,
            account=None,
            query_id=str(access_event.query_id),
            default_database=access_event.database,
        )

        return process_and_init_query_log(
            query=access_event.querytxt,
            platform=DataPlatform.REDSHIFT,
            process_query_config=self._query_log_config.process_query,
            query_log=PartialQueryLog(
                start_time=access_event.start_time,
                duration=float(
                    (access_event.end_time - access_event.start_time).total_seconds()
                ),
                user_id=access_event.usename,
                rows_read=float(access_event.rows),
                bytes_read=float(access_event.bytes),
                sources=tll.sources,
                targets=tll.targets,
            ),
            query_id=str(access_event.query_id),
        )
