from typing import Collection, Dict, List, Optional, Tuple

from asyncpg import Connection
from sqllineage.core.models import Schema, Table
from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

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
)
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.lineage.config import RedshiftLineageRunConfig
from metaphor.redshift.utils import exclude_system_databases

logger = get_logger()


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
        self._filter = exclude_system_databases(self._filter)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching lineage info from redshift host {self._host}")

        databases = (
            await self._fetch_databases()
            if self._filter.includes is None
            else list(self._filter.includes.keys())
        )

        for db in databases:
            if not self._filter.include_database(db):
                logger.info(f"Skipping database {db}")
                continue

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

    async def _fetch_lineage_from_stl_query(self, conn) -> None:
        sql = """
        WITH
        full_queries AS (
            SELECT
                query,
                LISTAGG(CASE WHEN LEN(RTRIM(text)) = 0 THEN text ELSE RTRIM(text) END, '') within group (order by sequence) AS querytxt
            FROM pg_catalog.stl_querytext
            WHERE sequence < 163
            GROUP BY query
        ),
        queries AS (
            SELECT *
            FROM pg_catalog.stl_query
            WHERE userid > 1
                AND (
                    querytxt ILIKE 'create table %% as select %%'
                    OR querytxt ILIKE 'insert %%'
                )
                AND aborted = 0

        )
        SELECT DISTINCT
            trim(fq.querytxt) AS querytxt,
            trim(q.database) AS database
        FROM
            queries AS q
            INNER JOIN full_queries AS fq
            ON q.query = fq.query
        ORDER BY q.endtime DESC;
        """
        results = await conn.fetch(sql)

        for row in results:
            query = row["querytxt"]
            database = row["database"]

            try:
                unescaped_query = query.encode().decode("unicode-escape")
                self._populate_lineage_from_sql(unescaped_query, database)
            except SQLLineageException as e:
                logger.warning(f"Cannot parse SQL. Query: {query}, Error: {e}")
                return

    def _populate_lineage_from_sql(self, query, database) -> None:
        def format_table_name(table: Table, database: str) -> str:
            return f"{database}.{str(table)}"

        parser = LineageRunner(query)

        if len(parser.target_tables) != 1:
            logger.warning(f"Cannot extract lineage for the query: {query}")
            return

        if len(parser.source_tables) < 1:
            return

        if any(
            [
                table.schema.raw_name == Schema.unknown
                for table in set(parser.source_tables + parser.target_tables)
            ]
        ):
            # TODO: find the default schema name
            logger.warning(f"Skip query missing explicit schema: {query}")
            return

        target = format_table_name(parser.target_tables[0], database)
        sources = [format_table_name(table, database) for table in parser.source_tables]

        if (not self._include_self_lineage) and target in sources:
            return

        source_ids = [
            str(to_dataset_entity_id(source, DataPlatform.REDSHIFT))
            for source in sources
        ]

        self._init_dataset_with_upstream(
            target,
            DatasetUpstream(source_datasets=source_ids, transformation=query),
        )

    async def _fetch_lineage(self, sql, conn, db) -> None:
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
            self._init_dataset_with_upstream(
                target_table_name,
                DatasetUpstream(
                    source_datasets=unique_list(sources), transformation=query
                ),
            )

    def _init_dataset_with_upstream(
        self, table_name: str, upstream: DatasetUpstream
    ) -> Dataset:
        if table_name not in self._datasets:
            self._datasets[table_name] = Dataset(
                logical_id=DatasetLogicalID(
                    name=table_name, platform=DataPlatform.REDSHIFT
                ),
                upstream=upstream,
            )
        return self._datasets[table_name]
