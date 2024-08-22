from typing import Collection, Iterator, List

from sqlalchemy import Connection, Inspector, text

from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.utils import md5_digest, safe_float, start_of_day, to_utc_time
from metaphor.database.extractor import GenericDatabaseExtractor
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    MaterializationType,
    QueryLog,
)
from metaphor.oracle.config import OracleRunConfig

logger = get_logger()

RDSADMIN_USER = "RDSADMIN"

EXCLUDE_SYSTEM_TABLE_CLAUSE = f"""
WHERE
  owner NOT IN (
    SELECT USERNAME FROM all_users WHERE ORACLE_MAINTAINED = 'Y'
  )
AND
  owner NOT in ('{RDSADMIN_USER}')
"""


class OracleExtractor(GenericDatabaseExtractor):
    """Oracle metadata extractor"""

    _description = "Oracle metadata crawler"
    _platform = Platform.ORACLE
    _sqlalchemy_dialect = "oracle+oracledb"

    @staticmethod
    def from_config_file(config_file: str) -> "OracleExtractor":
        return OracleExtractor(OracleRunConfig.from_yaml_file(config_file))

    def __init__(self, config: OracleRunConfig):
        super().__init__(config)
        self._query_logs_config = config.query_logs

        self._database = config.database.lower()

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from Oracle host {self._config.host}")

        inspector = OracleExtractor.get_inspector(self._get_sqlalchemy_url())

        system_users = self.get_system_users(inspector)
        system_users.extend([RDSADMIN_USER.lower()])

        for schema in self._get_schemas(inspector):
            if schema.lower() in system_users:
                continue
            self._extract_schema(inspector, schema)

        self._extract_ddl(inspector)
        self._extract_foreign_key(inspector)

        return self._datasets.values()

    def collect_query_logs(self) -> Iterator[QueryLog]:
        lookback_days = self._query_logs_config.lookback_days
        if lookback_days <= 0:
            return

        logger.info(f"Fetching queries from the last {lookback_days} days")

        inspector = OracleExtractor.get_inspector(self._get_sqlalchemy_url())

        # Exclude all system user
        excluded_users = self.get_system_users(inspector)

        # Exclude RDS admin user
        excluded_users.append(RDSADMIN_USER)

        # Exclude metaphor user
        excluded_users.append(self._config.user)

        # Exclude user-specified excluded usernames
        excluded_users.extend(self._query_logs_config.excluded_usernames)

        for query_log in self._extract_query_logs(inspector, excluded_users):
            yield query_log

    def _extract_schema(self, inspector: Inspector, schema: str):
        if not self._filter.include_schema_two_level(schema):
            logger.info(f"Skip schema: {schema}")
            return

        for table in inspector.get_table_names(schema):
            self._extract_table(inspector, schema, table)

        for view in self._extract_views_names(inspector, schema):
            self._extract_table(
                inspector, schema, view, materialization=MaterializationType.VIEW
            )

        for view in self._extract_mviews_names(inspector, schema):
            self._extract_table(
                inspector,
                schema,
                view,
                materialization=MaterializationType.MATERIALIZED_VIEW,
            )

    def get_system_users(self, inspector: Inspector):
        with inspector.engine.connect() as connection:
            sql = "SELECT USERNAME FROM all_users WHERE ORACLE_MAINTAINED = 'Y'"
            cursor = connection.execute(text(sql))
            rows = cursor.fetchall()
            return [user.lower() for user, in rows]

    def _extract_views_names(self, inspector: Inspector, schema: str):
        with inspector.engine.connect() as connection:
            sql = f"""
            SELECT VIEW_NAME FROM all_views
            WHERE OWNER = '{schema.upper()}'
            """
            cursor = connection.execute(text(sql))
            rows = cursor.fetchall()
            return [view_name.lower() for view_name, in rows]

    def _extract_mviews_names(self, inspector: Inspector, schema: str):
        with inspector.engine.connect() as connection:
            sql = f"""
            SELECT MVIEW_NAME FROM all_mviews
            WHERE OWNER = '{schema.upper()}'
            """
            cursor = connection.execute(text(sql))
            rows = cursor.fetchall()
            return [mview_name.lower() for mview_name, in rows]

    def _extract_ddl(self, inspector: Inspector):
        engine = inspector.engine

        with engine.connect() as connection:
            sql = f"""
            SELECT
              table_name as "name",
              owner,
              DBMS_METADATA.GET_DDL('TABLE', table_name, owner) AS ddl
            FROM all_tables
            {EXCLUDE_SYSTEM_TABLE_CLAUSE}
            UNION ALL
            SELECT
              view_name as "name",
              owner,
              DBMS_METADATA.GET_DDL('VIEW', view_name, owner) AS ddl
            FROM all_views
            {EXCLUDE_SYSTEM_TABLE_CLAUSE}
            UNION ALL
            SELECT
              mview_name as "name",
              owner,
              DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW', mview_name, owner) AS ddl
            FROM all_mviews
            {EXCLUDE_SYSTEM_TABLE_CLAUSE}
            """
            cursor = connection.execute(text(sql))
            rows = cursor.fetchall()

            for table, schema, ddl in rows:
                table_name = dataset_normalized_name(
                    db=self._database, schema=schema, table=table
                )
                dataset = self._datasets.get(table_name)
                if dataset is None:
                    continue
                assert dataset.schema and dataset.schema.sql_schema
                dataset.schema.sql_schema.table_schema = ddl

    def _inner_fetch_query_logs(
        self, sql: str, connection: Connection
    ) -> List[QueryLog]:
        cursor = connection.execute(text(sql))

        rows = cursor.fetchall()
        logs: List[QueryLog] = []
        for (
            user,
            query,
            start,
            duration,
            query_id,
            read_bytes,
            write_bytes,
            row_count,
        ) in rows:
            schema = user.lower() if user else None
            database = self._database if self._database else None

            tll = extract_table_level_lineage(
                query,
                platform=DataPlatform.ORACLE,
                account=self._alternative_host or self._config.host,
                query_id=query_id,
                default_database=database,
                default_schema=schema,
            )

            logs.append(
                QueryLog(
                    id=f"{DataPlatform.ORACLE.name}:{query_id}",
                    query_id=query_id,
                    platform=DataPlatform.ORACLE,
                    default_database=database,
                    default_schema=schema,
                    user_id=user,
                    sql=query,
                    sql_hash=md5_digest(query.encode("utf-8")),
                    duration=float(duration),
                    start_time=to_utc_time(start),
                    bytes_read=safe_float(read_bytes),
                    bytes_written=safe_float(write_bytes),
                    sources=tll.sources,
                    rows_read=safe_float(row_count),
                    targets=tll.targets,
                )
            )
        return logs

    def _extract_query_logs(self, inspector: Inspector, excluded_users: List[str]):
        start_time = start_of_day(
            daysAgo=self._query_logs_config.lookback_days
        ).strftime("%y-%m-%d/%H:%M:%S")

        users = [f"'{user.upper()}'" for user in excluded_users]

        with inspector.engine.connect() as connection:
            offset = 0
            result_limit = 1000
            filters = f"""AND PARSING_SCHEMA_NAME not in ({','.join(users)})"""

            while True:
                sql = f"""
                SELECT
                  PARSING_SCHEMA_NAME,
                  SQL_FULLTEXT AS query_text,
                  TO_TIMESTAMP(FIRST_LOAD_TIME, 'yy-MM-dd/HH24:MI:SS') AS start_time,
                  ELAPSED_TIME / 1000000 AS duration,
                  SQL_ID,
                  PHYSICAL_READ_BYTES,
                  PHYSICAL_WRITE_BYTES,
                  ROWS_PROCESSED
                FROM gv$sql
                WHERE OBJECT_STATUS = 'VALID'
                  {filters}
                  AND TO_TIMESTAMP(FIRST_LOAD_TIME, 'yy-MM-dd/HH24:MI:SS') >= TO_TIMESTAMP('{start_time}', 'yy-MM-dd HH24:MI:SS')
                ORDER BY FIRST_LOAD_TIME DESC
                OFFSET {offset} ROWS FETCH NEXT {offset + result_limit} ROWS ONLY
                """
                logs = self._inner_fetch_query_logs(sql, connection)
                for log in logs:
                    yield log

                logger.info(f"Fetched {len(logs)} query logs")

                if len(logs) < result_limit:
                    break
                offset += result_limit
