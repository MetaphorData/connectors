from datetime import datetime
from typing import Any, Dict, Iterable, List

import pymssql

from metaphor.common.api_request import get_request
from metaphor.common.logger import get_logger
from metaphor.synapse.model import (
    DedicatedSqlPoolSchema,
    DedicatedSqlPoolTable,
    QueryLogTable,
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

    def get_sql_pool_query_logs(
        self, username: str, password: str, start_date: datetime, end_date: datetime
    ) -> Iterable[QueryLogTable]:
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
        rows = self.__sql_query_fetch_all(
            f"{self._sql_on_demand_query_endpoint}", username, password, query_str
        )
        for row in rows:
            yield QueryLogTable(
                request_id=row[0],
                sql_query=row[1],
                login_name=row[2],
                start_time=QueryLogTable.to_utc_time(row[3]),
                end_time=QueryLogTable.to_utc_time(row[4]),
                duration=row[5],
                query_size=row[6],
                error=row[7],
                query_operation=row[8],
            )

    def get_dedicated_sql_pool_query_logs(
        self,
        database: str,
        username: str,
        password: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Iterable[QueryLogTable]:
        query_str = """
            SELECT r.request_id, r.session_id, r.command, r.start_time, r.end_time, r.total_elapsed_time,
            stps.row_count, s.login_name, e.error_id, e.details as error
            FROM sys.dm_pdw_exec_requests r
            INNER JOIN sys.dm_pdw_exec_sessions s ON r.session_id = s.session_id
            LEFT JOIN sys.dm_pdw_errors e ON r.session_id = e.session_id
            LEFT JOIN sys.dm_pdw_request_steps stps ON r.request_id = stps.request_id
            WHERE stps.location_type in ('Compute', 'DMS')
            AND s.login_name LIKE '%live.com%'
            AND r.start_time > '{}' AND r.start_time < '{}'
            AND s.session_id NOT IN (
                SELECT DISTINCT session_id FROM sys.dm_pdw_exec_requests  WHERE command LIKE '%@@Azure.Synapse.Monitoring.SQLQuerylist%'
            )
            ORDER BY r.start_time DESC;
        """.format(
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            end_date.strftime("%Y-%m-%d %H:%M:%S"),
        )
        rows = self.__sql_query_fetch_all(
            f"{self._sql_query_endpoint}", username, password, query_str, database
        )
        for row in rows:
            operation = (row[2].split(" "))[0]
            yield QueryLogTable(
                request_id=row[0],
                session_id=row[1],
                sql_query=row[2],
                start_time=QueryLogTable.to_utc_time(row[3]),
                end_time=QueryLogTable.to_utc_time(row[4]),
                duration=row[5],
                row_count=row[6],
                login_name=row[7],
                error=row[8],
                query_operation=operation,
            )

    def __sql_query_fetch_all(
        self, endpoint, username: str, password: str, query_str: str, database: str = ""
    ) -> List[Any]:
        server = endpoint
        database_str = f"{database}" if len(database) > 0 else ""
        conn = pymssql.connect(
            server=server,
            user=f'{username}@{endpoint.split(".")[0]}',
            password=password,
            database=database_str,
            conn_properties="",
        )
        cursor = conn.cursor()
        cursor.execute(query_str)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
