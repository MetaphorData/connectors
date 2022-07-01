from typing import Collection, Dict, List, Optional, Tuple

from asyncpg import Connection
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUpstream,
    EntityType,
)

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import DatasetFilter
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.lineage.config import RedshiftLineageRunConfig

logger = get_logger(__name__)


class RedshiftLineageExtractor(PostgreSQLExtractor):
    """Redshift lineage metadata extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.REDSHIFT

    def description(self) -> str:
        return "Redshift data lineage crawler"

    @staticmethod
    def config_class():
        return RedshiftLineageRunConfig

    def __init__(self):
        super().__init__()
        self._platform = DataPlatform.REDSHIFT
        self._dataset_filter: DatasetFilter = DatasetFilter()

    async def extract(
        self, config: RedshiftLineageRunConfig
    ) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching lineage info from redshift host {config.host}")

        self._dataset_filter = config.filter.normalize()

        databases = (
            await self._fetch_databases(config)
            if self._dataset_filter.includes is None
            else list(self._dataset_filter.includes.keys())
        )

        for db in databases:
            conn = await PostgreSQLExtractor._connect_database(config, db)

            if config.enable_lineage_from_stl_scan:
                await self._fetch_upstream_from_stl_scan(conn, db)

            if config.enable_view_lineage:
                await self._fetch_view_upstream(conn, db)

            await conn.close()

        return self._datasets.values()

    async def _fetch_upstream_from_stl_scan(self, conn: Connection, db: str) -> None:
        stl_scan_based_lineage_query: str = """
            SELECT DISTINCT
                target_schema, target_table, source_schema, source_table, trim(sq.querytxt) as querytxt
            FROM
                    (
                SELECT
                    trim(nspname) AS target_schema, trim(relname) AS target_table, query
                FROM
                    stl_insert
                JOIN pg_class ON pg_class.oid = tbl
                JOIN pg_namespace ON pg_namespace.oid = relnamespace
                    ) AS target
            JOIN
                    (
                SELECT
                    trim(nspname) AS source_schema, trim(relname) AS source_table, query
                FROM stl_scan
                JOIN pg_class ON pg_class.oid = tbl
                JOIN pg_namespace ON pg_namespace.oid = relnamespace
                WHERE
                    userid > 1 AND type in (1, 2, 3)
                    ) AS source
            USING (query)
            LEFT JOIN stl_query sq ON target.query = sq.query
            WHERE
                target_schema != 'information_schema'
                AND target_schema !~ '^pg_'
                AND source_schema != 'information_schema'
                AND source_schema !~ '^pg_'
        """
        await self._fetch_lineage(stl_scan_based_lineage_query, conn, db)

    async def _fetch_view_upstream(self, conn: Connection, db: str) -> None:
        view_lineage_query: str = """
            SELECT DISTINCT
                snsp.nspname AS source_schema, spgc.relname AS source_table,
                tnsp.nspname AS target_schema, tpgc.relname AS target_table
            FROM
                pg_catalog.pg_class AS spgc
            INNER JOIN
                pg_catalog.pg_depend AS spgd
                    ON
                spgc.oid = spgd.refobjid
            INNER JOIN
                pg_catalog.pg_depend AS tpgd
                    ON
                spgd.objid = tpgd.objid
            JOIN
                pg_catalog.pg_class AS tpgc
                    ON
                tpgd.refobjid = tpgc.oid
                AND spgc.oid <> tpgc.oid
            LEFT OUTER JOIN
                pg_catalog.pg_namespace AS snsp
                    ON
                spgc.relnamespace = snsp.oid
            LEFT OUTER JOIN
                pg_catalog.pg_namespace tnsp
                    ON
                tpgc.relnamespace = tnsp.oid
            WHERE
                tpgd.deptype = 'i'
                AND tpgc.relkind = 'v'
                AND tnsp.nspname != 'information_schema'
                AND tnsp.nspname !~ '^pg_'
                AND snsp.nspname != 'information_schema'
                AND snsp.nspname !~ '^pg_'
                ORDER BY target_schema, target_table ASC;
        """
        await self._fetch_lineage(view_lineage_query, conn, db)

    async def _fetch_lineage(self, sql, conn, db):
        results = await conn.fetch(sql)

        upstream_map: Dict[str, Tuple[List[str], Optional[str]]] = {}

        for row in results:
            source_table_name = f"{db}.{row['source_schema']}.{row['source_table']}"
            target_table_name = f"{db}.{row['target_schema']}.{row['target_table']}"
            query = row["querytxt"].rstrip() if "querytxt" in row else None

            source_id = str(
                to_dataset_entity_id(source_table_name, DataPlatform.REDSHIFT)
            )
            upstream_map.setdefault(target_table_name, (list(), query))[0].append(
                source_id
            )

        for target_table_name in upstream_map.keys():
            dataset = self._init_dataset(target_table_name)
            sources, query = upstream_map[target_table_name]
            dataset.upstream = DatasetUpstream(
                source_datasets=unique_list(sources), transformation=query
            )

    def _init_dataset(self, table_name: str) -> Dataset:
        if table_name not in self._datasets:
            self._datasets[table_name] = Dataset(
                entity_type=EntityType.DATASET,
                logical_id=DatasetLogicalID(
                    name=table_name, platform=DataPlatform.REDSHIFT
                ),
            )

        return self._datasets[table_name]
