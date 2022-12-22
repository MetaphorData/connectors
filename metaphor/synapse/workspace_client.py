from datetime import datetime
from typing import Any, Dict, Iterable, List

from metaphor.common.logger import get_logger
from metaphor.common.utils import to_utc_time
from metaphor.synapse.model import (
    SynapseColumn,
    SynapseDatabase,
    SynapseQueryLog,
    SynapseTable,
)

try:
    import pymssql
except ImportError:
    print("Please install metaphor[synapse] extra\n")
    raise

logger = get_logger()


class WorkspaceClient:
    def __init__(
        self,
        workspace_name: str,
        username: str,
        password: str,
    ):
        self.workspace_name = workspace_name
        self._sql_query_endpoint = f"{workspace_name}.sql.azuresynapse.net"
        self._sql_on_demand_query_endpoint = (
            f"{workspace_name}-ondemand.sql.azuresynapse.net"
        )
        self._username = username
        self._password = password

    def get_databases(self, is_dedicated: bool = False) -> Iterable[SynapseDatabase]:
        query_str = """
            SELECT database_id, name, create_date, collation_name from sys.databases where name != 'master';
        """
        end_point = (
            self._sql_query_endpoint
            if is_dedicated
            else self._sql_on_demand_query_endpoint
        )
        rows = self._sql_query_fetch_all(end_point, query_str)
        for row in rows:
            yield SynapseDatabase(
                id=row[0],
                name=row[1],
                create_time=to_utc_time(row[2]),
                collation_name=row[3],
            )

    def get_tables(
        self, database_name: str, is_dedicated: bool = False
    ) -> List[SynapseTable]:
        query_str = """
        SELECT t.object_id, t.name AS table_name, s.name AS schema_name
            ,t.type, t.create_date
            ,c.name AS column_name, c.max_length, c.precision
            ,c.is_nullable, typ.name as column_type
            ,idx.is_unique, idx.is_primary_key
            ,t.is_external, fky.foreign_key_name
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        INNER JOIN sys.columns AS c ON t.object_id = c.object_id
        INNER JOIN sys.types As typ ON c.user_type_id = typ.user_type_id
        LEFT JOIN (
            SELECT ic.object_id, ic.column_id, i.is_unique, i.is_primary_key
            FROM sys.indexes AS i
            INNER JOIN sys.index_columns AS ic
                ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        ) AS idx ON t.object_id = idx.object_id AND c.column_id = idx.column_id
        LEFT JOIN(
            SELECT fk.name AS foreign_key_name
                , fk.referenced_object_id as object_id
                , fkc.parent_column_id as column_id
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc
                ON fkc.constraint_object_id = fk.object_id
        ) AS fky ON t.object_id = fky.object_id and c.column_id = fky.column_id
        """
        end_point = (
            self._sql_query_endpoint
            if is_dedicated
            else self._sql_on_demand_query_endpoint
        )
        rows = self._sql_query_fetch_all(end_point, query_str, database_name)

        table_dict: Dict[str, SynapseTable] = {}

        for row in rows:
            if row[0] not in table_dict:
                table = SynapseTable(
                    id=row[0],
                    name=row[1],
                    schema_name=row[2],
                    type=row[3],
                    create_time=to_utc_time(row[4]),
                    column_dict=[],
                    is_external=row[12],
                )
                if table.is_external:
                    query_str = f"""
                        SELECT exd.location AS source, exf.format_type
                        FROM sys.external_tables AS ext
                        LEFT JOIN sys.external_data_sources AS exd
                        ON ext.data_source_id = exd.data_source_id
                        LEFT JOIN sys.external_file_formats AS exf
                        ON ext.file_format_id = exf.file_format_id
                        WHERE ext.object_id = '{row[0]}';
                    """
                    rows = self._sql_query_fetch_all(
                        end_point, query_str, database_name
                    )
                    if len(rows) == 1:
                        table.external_source = rows[0][0]
                        table.external_file_format = rows[0][1]

                table_dict[row[0]] = table
            if row[5] in table_dict[row[0]].column_dict:
                if row[10]:
                    table_dict[row[0]].column_dict[row[5]].is_unique = True
                if row[11]:
                    table_dict[row[0]].column_dict[row[5]].is_primary_key = True
            else:
                table_dict[row[0]].column_dict[row[5]] = SynapseColumn(
                    name=row[5],
                    max_length=float(row[6]),
                    precision=float(row[7]),
                    is_nullable=row[8],
                    type=row[9],
                    is_unique=row[10],
                    is_primary_key=row[11],
                    is_foreign_key=row[13],
                )

        return list(table_dict.values())

    def get_sql_pool_query_logs(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[SynapseQueryLog]:
        query_str = """
        SELECT transaction_id, query_text as query_string, login_name, start_time, end_time,
            total_elapsed_time_ms as elapsed_time, data_processed_mb as data_size_mb,
            error, error_code, rejected_rows_path, command
        FROM sys.dm_exec_requests_history AS h
        WHERE h.login_name LIKE '%live.com#%'
            AND h.start_time > '{}' AND h.start_time < '{}'
        ORDER BY h.start_time DESC
        """.format(
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            end_date.strftime("%Y-%m-%d %H:%M:%S"),
        )
        rows = self._sql_query_fetch_all(self._sql_on_demand_query_endpoint, query_str)
        for row in rows:
            yield SynapseQueryLog(
                request_id=row[0],
                sql_query=row[1],
                login_name=row[2],
                start_time=to_utc_time(row[3]),
                end_time=to_utc_time(row[4]),
                duration=row[5],
                query_size=row[6],
                error=row[7],
                query_operation=row[8],
            )

    def get_dedicated_sql_pool_query_logs(
        self,
        database: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Iterable[SynapseQueryLog]:
        query_str = """
        SELECT r.request_id, r.session_id, r.command, r.start_time, r.end_time, r.total_elapsed_time,
            stps.row_count, s.login_name, e.error_id, e.details AS error
        FROM sys.dm_pdw_exec_requests r
        INNER JOIN sys.dm_pdw_exec_sessions s
            ON r.session_id = s.session_id
        LEFT JOIN sys.dm_pdw_errors e
            ON r.session_id = e.session_id
        LEFT JOIN sys.dm_pdw_request_steps stps
            ON r.request_id = stps.request_id
        WHERE stps.location_type IN ('Compute', 'DMS')
            AND s.login_name LIKE '%live.com%'
            AND r.start_time > '{}' AND r.start_time < '{}'
            AND s.session_id NOT IN (
                SELECT DISTINCT session_id FROM sys.dm_pdw_exec_requests WHERE command LIKE '%@@Azure.Synapse.Monitoring.SQLQuerylist%'
            )
        ORDER BY r.start_time DESC;
        """.format(
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            end_date.strftime("%Y-%m-%d %H:%M:%S"),
        )
        rows = self._sql_query_fetch_all(self._sql_query_endpoint, query_str, database)
        for row in rows:
            operation = (row[2].split(" "))[0]
            yield SynapseQueryLog(
                request_id=row[0],
                session_id=row[1],
                sql_query=row[2],
                start_time=to_utc_time(row[3]),
                end_time=to_utc_time(row[4]),
                duration=row[5],
                row_count=row[6],
                login_name=row[7],
                error=row[8],
                query_operation=operation,
            )

    def _sql_query_fetch_all(
        self, endpoint: str, query_str: str, database: str = ""
    ) -> List[Any]:
        rows = []
        try:
            server = endpoint
            database_str = f"{database}" if len(database) > 0 else ""
            conn = pymssql.connect(
                server=server,
                user=f'{self._username}@{endpoint.split(".")[0]}',
                password=self._password,
                database=database_str,
                conn_properties="",
            )
            cursor = conn.cursor()
            cursor.execute(query_str)
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
        except Exception as ex:
            logger.exception(
                f"endpoint[{endpoint}] \n database:[{database}] \n sql query error: {ex}"
            )
        finally:
            return rows
