from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from metaphor.common.api_request import get_request
from metaphor.common.logger import get_logger

logger = get_logger()


class SynapseDataModel(BaseModel):
    id: str
    name: str
    type: str


class SynapseWorkspace(SynapseDataModel):
    properties: Any


class WorkspaceDatabase(SynapseDataModel):
    properties: Any


class DedicatedSqlPoolSchema(SynapseDataModel):
    pass


class DedicatedSqlPoolColumn(SynapseDataModel):
    properties: Any


class DedicatedSqlPoolTable(SynapseDataModel):
    properties: Any
    sqlSchema: Optional[DedicatedSqlPoolSchema]
    columns: Optional[List]


class SynapseTable(SynapseDataModel):
    properties: Any


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

    def get_spark_monitor(self):
        # https://meteaphor-workspace.dev.azuresynapse.net/monitoring/workloadTypes/spark/applications?api-version=2020-10-01-preview&filter=(submitTime%20ge%202022-10-26T18:52:29Z%20and%20submitTime%20le%202022-10-27T18:52:29Z)&skip=0
        pass

    def get_sql_monitor(self):
        # sql query
        # https://meteaphor-workspace.dev.azuresynapse.net/monitoring/workloadTypes/sql/querystring?api-version=2020-10-01-preview&use-workspace-token=true&isGen3Pool=false&filter=(((PoolName%20eq%20%27On-demand%27))%20and%20(submitTime%20ge%202022-10-26T18:57:15Z%20and%20submitTime%20le%202022-10-27T18:57:15Z))&skip=0
        # sql
        # https://meteaphor-workspace-ondemand.sql.azuresynapse.net//databases/master/query?api-version=2018-08-01-preview&application=AzureSynapseMonitoring
        pass
