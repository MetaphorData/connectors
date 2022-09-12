from typing import Collection, Dict, List, Optional, Tuple

from asyncpg import Connection
from sql_metadata import Parser

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetUpstream,
    EntityType,
)
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.lineage.config import RedshiftLineageRunConfig

logger = get_logger(__name__)


class RedshiftLineageExtractor(PostgreSQLExtractor):
    """Redshift lineage metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftLineageExtractor":
        return RedshiftLineageExtractor(
            RedshiftLineageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: RedshiftLineageRunConfig):
        super().__init__(
            config,
            "Redshift data lineage crawler",
            Platform.REDSHIFT,
            DataPlatform.REDSHIFT,
        )
        self._database = config.database
        self._enable_lineage_from_sql = config.enable_lineage_from_sql
        self._enable_view_lineage = config.enable_view_lineage
        self._include_self_lineage = config.include_self_lineage

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching lineage info from redshift host {self._host}")

        databases = (
            await self._fetch_databases()
            if self._filter.includes is None
            else list(self._filter.includes.keys())
        )

        for db in databases:
            conn = await self._connect_database(db)

            if self._enable_view_lineage:
                await self._fetch_view_upstream(conn, db)

            await conn.close()

        if self._enable_lineage_from_sql:
            conn = await self._connect_database(self._database)
            await self._fetch_lineage_from_stl_query(conn)
            await conn.close()

        return self._datasets.values()

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

    async def _fetch_lineage_from_stl_query(self, conn):
        sql = """
        SELECT DISTINCT
            trim(q.querytxt) AS querytxt,
            trim(q.database) AS database
        FROM (
            SELECT *
            FROM pg_catalog.stl_query
            WHERE userid > 1
                AND (
                    querytxt ILIKE 'create table %% as select %%'
                    OR querytxt ILIKE 'insert %%'
                )
                AND aborted = 0
        ) q
        ORDER BY q.endtime DESC;
        """
        results = await conn.fetch(sql)

        def format_table_name(table: str, database: str) -> str:
            tokens = table.split(".")
            if len(tokens) == 1:
                return ".".join([database, "public", tokens[0]])
            elif len(tokens) == 2:
                return ".".join([database] + tokens)
            else:
                logger.warning(f"formatting table name error: {table}")
                return ""

        for row in results:
            query = row["querytxt"]
            database = row["database"]
            parser = Parser(query.replace('"', ""))

            tables = [format_table_name(table, database) for table in parser.tables]

            target, *sources = tables

            # Only one table in a Insert/Create Table query
            if len(tables) == 1:
                if self._include_self_lineage:
                    target, sources = tables[0], tables
                else:
                    # Skip self lineage
                    continue

            source_ids = [
                str(to_dataset_entity_id(source, DataPlatform.REDSHIFT))
                for source in sources
            ]

            self._init_dataset(
                target,
                DatasetUpstream(source_datasets=source_ids, transformation=query),
            )

    async def _fetch_lineage(self, sql, conn, db):
        results = await conn.fetch(sql)

        upstream_map: Dict[str, Tuple[List[str], Optional[str]]] = {}

        for row in results:
            source_table_name = f"{db}.{row['source_schema']}.{row['source_table']}"
            target_table_name = f"{db}.{row['target_schema']}.{row['target_table']}"

            # Ignore self-lineage
            if (
                not self._include_self_lineage
                and source_table_name.lower() == target_table_name.lower()
            ):
                continue

            query = row["querytxt"].rstrip() if row.get("querytxt") else None

            source_id = str(
                to_dataset_entity_id(source_table_name, DataPlatform.REDSHIFT)
            )
            upstream_map.setdefault(target_table_name, (list(), query))[0].append(
                source_id
            )

        for target_table_name in upstream_map.keys():
            sources, query = upstream_map[target_table_name]
            self._init_dataset(
                target_table_name,
                DatasetUpstream(
                    source_datasets=unique_list(sources), transformation=query
                ),
            )

    def _init_dataset(self, table_name: str, upstream: DatasetUpstream) -> Dataset:
        if table_name not in self._datasets:
            self._datasets[table_name] = Dataset(
                entity_type=EntityType.DATASET,
                logical_id=DatasetLogicalID(
                    name=table_name, platform=DataPlatform.REDSHIFT
                ),
                upstream=upstream,
            )
        return self._datasets[table_name]
