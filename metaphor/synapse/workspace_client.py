from typing import Any, Dict, Iterable, List

import pyodbc

from metaphor.common.api_request import get_request
from metaphor.common.logger import get_logger
from metaphor.synapse.model import (
    DedicatedSqlPoolSchema,
    DedicatedSqlPoolTable,
    QueryTable,
    SynapseTable,
    SynapseWorkspace,
    WorkspaceDatabase,
)

logger = get_logger()


class WorkspaceClient:
    AZURE_MANGEMENT_ENDPOINT = "https://management.azure.com"

    def __init__(
        self,
        workspace: SynapseWorkspace,
        subscription_id: str,
        synapse_headers: Dict[str, str],
        management_headers: Dict[str, str],
    ):
        self._workspace = workspace
        self._subscription_id = subscription_id
        self._azure_synapse_headers = synapse_headers
        self._azure_management_headers = management_headers
        self._dev_endpoint = workspace.properties["connectivityEndpoints"]["dev"]
        self._sql_query_endpoint = workspace.properties["connectivityEndpoints"]["sql"]
        self._sql_on_demand_query_endpoint = workspace.properties[
            "connectivityEndpoints"
        ]["sqlOnDemand"]
        self._account_endpoint = workspace.properties["defaultDataLakeStorage"][
            "accountUrl"
        ]
        self._default_file_system = workspace.properties["defaultDataLakeStorage"][
            "filesystem"
        ]
        index1 = workspace.id.index("/resourceGroups/")
        index2 = workspace.id.index("/providers/")
        self._resource_group_name = workspace.id[index1 + 16 : index2]

    def get_databases(self):
        url = f"{self._dev_endpoint}/databases?api-version=2021-04-01"
        return get_request(
            url,
            self._azure_synapse_headers,
            List[WorkspaceDatabase],
            transform_response=lambda r: r.json()["items"],
        )

    def get_dedicated_sql_pool_databases(self):
        # https://learn.microsoft.com/en-us/rest/api/synapse/sql-pools/list-by-workspace?tabs=HTTP
        url = f"{self.AZURE_MANGEMENT_ENDPOINT}/subscriptions/{self._subscription_id}/resourceGroups/{self._resource_group_name}/providers/Microsoft.Synapse/workspaces/{self._workspace.name}/sqlPools?api-version=2021-06-01"
        return get_request(
            url,
            self._azure_management_headers,
            List[WorkspaceDatabase],
            transform_response=lambda r: r.json()["value"],
        )

    def get_tables(self, database_name: str) -> List[SynapseTable]:
        url = f"{self._dev_endpoint}/databases/{database_name}/tables?api-version=2021-04-01"
        return get_request(
            url,
            self._azure_synapse_headers,
            List[SynapseTable],
            transform_response=lambda r: r.json()["items"],
        )

    def get_dedicated_sql_pool_tables(
        self, database_name: str
    ) -> List[DedicatedSqlPoolTable]:
        api_version = "api-version=2021-06-01"
        # https://learn.microsoft.com/en-us/rest/api/synapse/sql-pool-schemas/list?tabs=HTTP
        url = f"{self.AZURE_MANGEMENT_ENDPOINT}/subscriptions/{self._subscription_id}/resourceGroups/{self._resource_group_name}/providers/Microsoft.Synapse/workspaces/{self._workspace.name}/sqlPools/{database_name}"
        sql_pool_tables = []
        schemas = get_request(
            f"{url}/schemas?{api_version}",
            self._azure_management_headers,
            List[DedicatedSqlPoolSchema],
            transform_response=lambda r: r.json()["value"],
        )

        for schema in schemas:
            tables = get_request(
                # https://learn.microsoft.com/en-us/rest/api/synapse/sql-pool-tables/list-by-schema?tabs=HTTP
                f"{url}/schemas/{schema.name}/tables?{api_version}",
                self._azure_management_headers,
                List[DedicatedSqlPoolTable],
                transform_response=lambda r: r.json()["value"],
            )

            for table in tables:
                table.sqlSchema = schema
                table.columns = get_request(
                    # https://learn.microsoft.com/en-us/rest/api/synapse/sql-pool-table-columns/list-by-table-name?tabs=HTTP
                    f"{url}/schemas/{schema.name}/tables/{table.name}/columns?{api_version}",
                    self._azure_management_headers,
                    List[Any],
                    transform_response=lambda r: r.json()["value"],
                )
                sql_pool_tables.append(table)
        return sql_pool_tables

    # def get_serverless_sql_pool_query_log(self, username:str, password:str, lookback_days: int) -> List[QueryTable]:
    def get_serverless_sql_pool_query_log(
        self, username: str, password: str, lookback_days: int
    ) -> Iterable[QueryTable]:
        driver = "{ODBC Driver 18 for SQL Server}"
        server = self._sql_on_demand_query_endpoint
        query_str = """
            SELECT h.status, h.transaction_id, h.query_hash, h.login_name, h.start_time, h.end_time,
            h.query_text as query_string, h.total_elapsed_time_ms as elapsed_time, data_processed_mb as data_size_mb,
            error, error_code, rejected_rows_path
            FROM sys.dm_exec_requests_history AS h
            WHERE h.login_name LIKE '%live.com#%'
            AND h.start_time > DATEADD(day,-{},GETDATE())
            ORDER BY h.start_time DESC
        """.format(
            lookback_days
        )
        connect_str = (
            "DRIVER="
            + driver
            + ";SERVER=tcp:"
            + server
            + ";PORT=1433"
            + ";UID="
            + username
            + ";PWD="
            + password
            + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        conn = pyodbc.connect(connect_str)
        cursor = conn.cursor()
        cursor.execute(query_str)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        for row in rows:
            yield QueryTable(
                request_id=row.transaction_id,
                sql_query=row.query_string,
                start_time=QueryTable.to_utc_time(row.start_time),
                end_time=QueryTable.to_utc_time(row.end_time),
                duration=row.elapsed_time,
                login_name=row.login_name,
                query_size=row.data_size_mb,
                error=row.error,
            )

    # check with muliple de-sql pool
    # def get_dedicated_sql_pool_query_log(self, database: str, username:str, password:str, lookback_days=0)-> List[QueryTable]:
    def get_dedicated_sql_pool_query_log(
        self, database: str, username: str, password: str, lookback_days=0
    ) -> Iterable[QueryTable]:
        server = f"{self._sql_query_endpoint}"
        query_str = """
            SELECT r.request_id, r.session_id, r.command, r.start_time, r.end_time, r.total_elapsed_time,
            stps.row_count, stps.location_type, stps.operation_type,
            s.login_name, s.app_name, s.query_count,
            e.error_id, e.details as error
            FROM sys.dm_pdw_exec_requests r
            INNER JOIN sys.dm_pdw_exec_sessions s ON r.session_id = s.session_id
            LEFT JOIN sys.dm_pdw_errors e ON r.session_id = e.session_id
            LEFT JOIN sys.dm_pdw_request_steps stps ON r.request_id = stps.request_id
            WHERE stps.location_type in ('Compute', 'DMS')
            AND s.login_name LIKE '%live.com%'
            AND r.start_time > DATEADD(day,-{},GETDATE())
            AND s.session_id NOT IN (
                SELECT DISTINCT session_id FROM sys.dm_pdw_exec_requests  WHERE command LIKE '%@@Azure.Synapse.Monitoring.SQLQuerylist%'
            )
            ORDER BY r.start_time DESC;
        """.format(
            lookback_days
        )
        driver = "{ODBC Driver 18 for SQL Server}"
        connect_str = (
            "DRIVER="
            + driver
            + ";SERVER=tcp:"
            + server
            + ";PORT=1433;DATABASE="
            + database
            + ";UID="
            + username
            + ";PWD="
            + password
            + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        conn = pyodbc.connect(connect_str)
        cursor = conn.cursor()
        cursor.execute(query_str)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        for row in rows:
            yield QueryTable(
                request_id=row.request_id,
                session_id=row.session_id,
                sql_query=row.command,
                login_name=row.login_name,
                start_time=QueryTable.to_utc_time(row.start_time),
                end_time=QueryTable.to_utc_time(row.end_time),
                duration=row.total_elapsed_time,
                error=row.error,
                row_count=row.row_count,
            )
