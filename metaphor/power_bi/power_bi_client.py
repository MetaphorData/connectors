import json
import tempfile
from enum import Enum
from time import sleep
from typing import Any, Callable, List, Optional, Type, TypeVar

import requests
from pydantic import BaseModel, parse_obj_as

from metaphor.common.logger import get_logger
from metaphor.power_bi.config import PowerBIRunConfig

try:
    import msal
except ImportError:
    print("Please install metaphor[power_bi] extra\n")
    raise


logger = get_logger(__name__)


class PowerBIApp(BaseModel):
    id: str
    name: str
    workspaceId: str


class PowerBIDataSource(BaseModel):
    datasourceType: str
    datasourceId: str
    connectionDetails: Any
    gatewayId: str


class PowerBIDataset(BaseModel):
    id: str
    name: str
    isRefreshable: bool
    webUrl: Optional[str]


class PowerBIDashboard(BaseModel):
    id: str
    displayName: str
    webUrl: Optional[str]


class PowerBIWorkspace(BaseModel):
    id: str
    name: str
    isReadOnly: bool
    type: str


class PowerBIReport(BaseModel):
    id: str
    name: str
    datasetId: Optional[str] = None
    reportType: str
    webUrl: Optional[str]


class PowerBIPage(BaseModel):
    name: str
    displayName: str
    order: int


class PowerBITile(BaseModel):
    id: str
    title: str = ""
    datasetId: str = ""
    reportId: str = ""
    embedUrl: Optional[str]


class PowerBIRefreshStatus(Enum):
    UNKNOWN = "Unknown"
    COMPLETED = "Completed"
    FAILED = "Failed"
    DISABLED = "Disabled"


class PowerBIRefresh(BaseModel):
    status: PowerBIRefreshStatus
    endTime: str


class PowerBITableColumn(BaseModel):
    name: str
    dataType: str = "unknown"


class PowerBITableMeasure(BaseModel):
    name: str
    description: Optional[str] = None
    expression: str = ""


class PowerBITable(BaseModel):
    name: str
    columns: List[PowerBITableColumn] = []
    measures: List[PowerBITableMeasure] = []
    source: List[Any] = []


class WorkspaceInfoDataset(BaseModel):
    id: str
    name: str
    tables: List[PowerBITable] = []

    description: str = ""
    ContentProviderType: str = ""
    CreatedDate: str = ""

    upstreamDataflows: Any
    upstreamDatasets: Any


class WorkspaceInfoDashboard(BaseModel):
    id: str
    appId: Optional[str] = None
    displayName: str


class WorkspaceInfoReport(BaseModel):
    id: str
    appId: Optional[str] = None
    name: str
    datasetId: Optional[str] = None
    description: str = ""


class WorkspaceInfo(BaseModel):
    id: str
    name: str
    type: str
    state: str
    reports: List[WorkspaceInfoReport] = []
    datasets: List[WorkspaceInfoDataset] = []
    dashboards: List[WorkspaceInfoDashboard] = []


class AuthenticationError(Exception):
    def __init__(self, body) -> None:
        super().__init__(
            f"Authentication error: {body}.\n"
            f"Please\n"
            f"  1. Enable Power BI admin read-only API for the app\n"
            f"  2. Enable service principal to use Power BI APIs for the app\n"
        )


class EntityNotFoundError(Exception):
    def __init__(self, body) -> None:
        super().__init__(f"EntityNotFound error: {body}")


class PowerBIClient:
    AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
    SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]
    API_ENDPOINT = "https://api.powerbi.com/v1.0/myorg"

    # Only include active workspaces. Ignore personal workspaces & legacy workspaces.
    GROUPS_FILTER = "state eq 'Active' and type eq 'Workspace'"

    # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info#request-body
    MAX_WORKSPACES_PER_SCAN = 100

    def __init__(self, config: PowerBIRunConfig):
        self._headers = {"Authorization": self.retrieve_access_token(config)}

    def retrieve_access_token(self, config: PowerBIRunConfig) -> str:
        app = msal.ConfidentialClientApplication(
            config.client_id,
            authority=self.AUTHORITY.format(tenant_id=config.tenant_id),
            client_credential=config.secret,
        )
        token = None
        token = app.acquire_token_silent(self.SCOPES, account=None)
        if not token:
            logger.info(
                "No suitable token exists in cache. Let's get a new one from AAD."
            )
            token = app.acquire_token_for_client(scopes=self.SCOPES)

        return f"Bearer {token['access_token']}"

    def get_groups(self) -> List[PowerBIWorkspace]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/groups-get-groups-as-admin
        url = f"{self.API_ENDPOINT}/admin/groups?$top=5000&$filter={PowerBIClient.GROUPS_FILTER}"
        return self._call_get(
            url, List[PowerBIWorkspace], transform_response=lambda r: r.json()["value"]
        )

    def get_apps(self) -> List[PowerBIApp]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/apps-get-apps-as-admin
        url = f"{self.API_ENDPOINT}/admin/apps?$top=5000"
        return self._call_get(
            url, List[PowerBIApp], transform_response=lambda r: r.json()["value"]
        )

    def get_tiles(self, dashboard_id: str) -> List[PowerBITile]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/dashboards-get-tiles-as-admin
        url = f"{self.API_ENDPOINT}/admin/dashboards/{dashboard_id}/tiles"
        return self._call_get(
            url, List[PowerBITile], transform_response=lambda r: r.json()["value"]
        )

    def get_datasets(self, group_id: str) -> List[PowerBIDataset]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/datasets-get-datasets-in-group-as-admin
        url = f"{self.API_ENDPOINT}/admin/groups/{group_id}/datasets"
        return self._call_get(
            url, List[PowerBIDataset], transform_response=lambda r: r.json()["value"]
        )

    def get_dashboards(self, group_id: str) -> List[PowerBIDashboard]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/dashboards-get-dashboards-in-group-as-admin
        url = f"{self.API_ENDPOINT}/admin/groups/{group_id}/dashboards"
        return self._call_get(
            url, List[PowerBIDashboard], transform_response=lambda r: r.json()["value"]
        )

    def get_reports(self, group_id: str) -> List[PowerBIReport]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/reports-get-reports-in-group-as-admin
        url = f"{self.API_ENDPOINT}/admin/groups/{group_id}/reports"
        return self._call_get(
            url, List[PowerBIReport], transform_response=lambda r: r.json()["value"]
        )

    def get_pages(self, group_id: str, report_id: str) -> List[PowerBIPage]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/reports/get-pages-in-group
        url = f"{self.API_ENDPOINT}/groups/{group_id}/reports/{report_id}/pages"

        try:
            return self._call_get(
                url, List[PowerBIPage], transform_response=lambda r: r.json()["value"]
            )
        except EntityNotFoundError as e:
            logger.error(
                f"Unable to find report {report_id} in workspace {group_id}\n"
                f"Please add the service principal as a viewer to the workspace"
            )
            raise e

    def get_refreshes(self, group_id: str, dataset_id: str) -> List[PowerBIRefresh]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group
        url = f"{self.API_ENDPOINT}/groups/{group_id}/datasets/{dataset_id}/refreshes"

        try:
            return self._call_get(
                url,
                List[PowerBIRefresh],
                transform_response=lambda r: r.json()["value"],
            )
        except EntityNotFoundError as e:
            logger.error(
                f"Unable to find dataset {dataset_id} in workspace {group_id}\n"
                f"Please add the service principal as a viewer to the workspace"
            )
            raise e

    def get_workspace_info(self, workspace_ids: List[str]) -> List[WorkspaceInfo]:
        def create_scan() -> str:
            # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info
            url = f"{self.API_ENDPOINT}/admin/workspaces/getInfo"
            request_body = {"workspaces": workspace_ids}
            result = requests.post(
                url,
                headers=self._headers,
                params={
                    "datasetExpressions": True,
                    "datasetSchema": True,
                    "datasourceDetails": True,
                    "getArtifactUsers": True,
                    "lineage": True,
                },
                data=request_body,
            )

            assert result.status_code == 202, (
                "Workspace scan create failed, "
                f"workspace_ids: {workspace_ids}, "
                f"response: [{result.status_code}] {result.content.decode()}"
            )

            scan_id = result.json()["id"]
            logger.info(f"Create a scan, id: {scan_id}")

            return result.json()["id"]

        def wait_for_scan_result(scan_id: str, max_timeout_in_secs: int = 30) -> bool:
            # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-status
            url = f"{self.API_ENDPOINT}/admin/workspaces/scanStatus/{scan_id}"

            waiting_time = 0
            sleep_time = 1
            while True:
                result = requests.get(url, headers=self._headers)
                if result.status_code != 200:
                    return False
                if result.json()["status"] == "Succeeded":
                    return True
                if waiting_time >= max_timeout_in_secs:
                    break
                waiting_time += sleep_time
                logger.info(f"Sleep {sleep_time} sec, wait for scan_id: {scan_id}")
                sleep(sleep_time)
            return False

        def transform_scan_result(response: requests.Response) -> dict:
            # Write output to file to help debug issues
            fd, name = tempfile.mkstemp(suffix=".json")
            with open(fd, "w") as fp:
                fp.write(response.text)
            logger.info(f"Scan result written to {name}")

            return response.json()["workspaces"]

        scan_id = create_scan()
        scan_success = wait_for_scan_result(scan_id)
        assert scan_success, f"Workspace scan failed, scan_id: {scan_id}"

        # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result
        url = f"{self.API_ENDPOINT}/admin/workspaces/scanResult/{scan_id}"
        return self._call_get(
            url,
            List[WorkspaceInfo],
            transform_response=transform_scan_result,
        )

    T = TypeVar("T")

    def _call_get(
        self,
        url: str,
        type_: Type[T],
        transform_response: Callable[[requests.Response], Any] = lambda r: r.json(),
    ) -> T:
        result = requests.get(url, headers=self._headers)
        body = result.content.decode()

        if result.status_code == 401:
            raise AuthenticationError(body)
        elif result.status_code == 404:
            raise EntityNotFoundError(body)
        elif result.status_code != 200:
            raise AssertionError(f"GET {url} failed: {result.status_code}\n{body}")

        logger.debug(f"Response from {url}:")
        logger.debug(json.dumps(result.json(), indent=2))
        return parse_obj_as(type_, transform_response(result))
