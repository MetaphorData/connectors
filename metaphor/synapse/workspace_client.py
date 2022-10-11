from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from metaphor.common.api_request import call_get


class SynapseDataModel(BaseModel):
    id: str
    name: str
    type: str


class SynapseWorkspace(SynapseDataModel):
    properties: Any


class WorkspaceDatabase(SynapseDataModel):
    properties: Any


class SqlPoolSchema(SynapseDataModel):
    pass


class SqlPoolColumn(SynapseDataModel):
    properties: Any


class SqlPoolTable(SynapseDataModel):
    properties: Any
    sqlSchema: Optional[SqlPoolSchema]
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
        self._dve_enpoint = workspace.properties["connectivityEndpoints"]["dev"]
        self._sql_query_endpoint = workspace.properties["connectivityEndpoints"]["sql"]
        self._sql_on_demand_query_endpoint = workspace.properties[
            "connectivityEndpoints"
        ]["sqlOnDemand"]
        self._accoutn_endpoint = workspace.properties["defaultDataLakeStorage"][
            "accountUrl"
        ]
        self._default_file_system = workspace.properties["defaultDataLakeStorage"][
            "filesystem"
        ]
        index1 = workspace.id.index("/resourceGroups/")
        index2 = workspace.id.index("/providers/")
        self._resource_group_name = workspace.id[index1 + 16 : index2]

    def get_databases(self):
        url = f"{self._dve_enpoint}/databases?api-version=2021-04-01"
        return call_get(
            url,
            self._azure_synapse_headers,
            List[WorkspaceDatabase],
            transform_response=lambda r: r.json()["items"],
        )

    def get_sqlpool_databases(self):
        url = f"{self._dve_enpoint}/sqlPools?api-version=2019-06-01-preview"
        return call_get(
            url,
            self._azure_synapse_headers,
            List[WorkspaceDatabase],
            transform_response=lambda r: r.json()["value"],
        )

    def get_tables(self, database_name: str) -> List[SynapseTable]:
        url = f"{self._dve_enpoint}/databases/{database_name}/tables?api-version=2021-04-01"
        return call_get(
            url,
            self._azure_synapse_headers,
            List[SynapseTable],
            transform_response=lambda r: r.json()["items"],
        )

    def get_sqlpool_tables(self, database_name: str) -> List[SqlPoolTable]:
        api_version = "api-version=2021-06-01"
        url = f"{self.AZURE_MANGEMENT_ENDPOINT}/subscriptions/{self._subscription_id}/resourceGroups/{self._resource_group_name}/providers/Microsoft.Synapse/workspaces/{self._workspace.name}/sqlPools/{database_name}"
        sql_pool_tables = []
        schemas = call_get(
            f"{url}/schemas?{api_version}",
            self._azure_management_headers,
            List[SqlPoolSchema],
            transform_response=lambda r: r.json()["value"],
        )

        for schema in schemas:
            tables = call_get(
                f"{url}/schemas/{schema.name}/tables?{api_version}",
                self._azure_management_headers,
                List[SqlPoolTable],
                transform_response=lambda r: r.json()["value"],
            )

            for table in tables:
                table.sqlSchema = schema
                table.columns = call_get(
                    f"{url}/schemas/{schema.name}/tables/{table.name}/columns?{api_version}",
                    self._azure_management_headers,
                    List[Any],
                    transform_response=lambda r: r.json()["value"],
                )
                sql_pool_tables.append(table)
        return sql_pool_tables

    def get_views(self, database):
        # https://meteaphor-workspace.dev.azuresynapse.net/ddl/database/default/view?api-version=2019-06-01-preview
        pass

    def get_links(self, storage=None):
        # https://metaphorstorageaccount.dfs.core.windows.net/metaphor-data-lake-container?recursive=false&resource=filesystem&_=1665184938548
        # GET exampleWorkspace.dev.azuresynapse.net/datasets/exampleDataset?api-version=2020-12-01

        pass
