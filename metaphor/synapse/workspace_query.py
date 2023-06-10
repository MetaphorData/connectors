from datetime import datetime
from typing import Iterator

from metaphor.common.logger import get_logger
from metaphor.common.utils import to_utc_time
from metaphor.mssql.model import MssqlConnectConfig
from metaphor.mssql.mssql_client import mssql_fetch_all
from metaphor.synapse.model import SynapseQueryLog

logger = get_logger()


class WorkspaceQuery:
    @staticmethod
    def get_sql_pool_query_logs(
        config: MssqlConnectConfig, start_date: datetime, end_date: datetime
    ) -> Iterator[SynapseQueryLog]:
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
        rows = mssql_fetch_all(config, query_str)
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

    @staticmethod
    def get_dedicated_sql_pool_query_logs(
        config: MssqlConnectConfig,
        database: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Iterator[SynapseQueryLog]:
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
        rows = mssql_fetch_all(config, query_str, database)
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
