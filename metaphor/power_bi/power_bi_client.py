from time import sleep
from typing import Any, Callable, List, Optional, Type, TypeVar

import requests
from pydantic import BaseModel

from metaphor.common.api_request import ApiError, get_request
from metaphor.common.logger import get_logger
from metaphor.power_bi.config import PowerBIRunConfig

try:
    import msal
except ImportError:
    print("Please install metaphor[power_bi] extra\n")
    raise


logger = get_logger()


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


class PowerBIRefresh(BaseModel):
    status: str = ""
    endTime: str = ""


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


class EndorsementDetails(BaseModel):
    endorsement: str
    certifiedBy: Optional[str] = ""


class UpstreamDataflow(BaseModel):
    targetDataflowId: str


class WorkspaceInfoDataset(BaseModel):
    id: str
    name: str
    tables: List[PowerBITable] = []

    description: str = ""
    ContentProviderType: str = ""
    CreatedDate: str = ""

    upstreamDataflows: Optional[List[UpstreamDataflow]] = None
    upstreamDatasets: Optional[Any]
    endorsementDetails: Optional[EndorsementDetails] = None


class WorkspaceInfoDashboardBase(BaseModel):
    id: str
    appId: Optional[str] = None
    createdDateTime: Optional[str] = None
    modifiedDateTime: Optional[str] = None
    createdBy: Optional[str] = None
    modifiedBy: Optional[str] = None
    endorsementDetails: Optional[EndorsementDetails] = None


class WorkspaceInfoDashboard(WorkspaceInfoDashboardBase):
    displayName: str


class WorkspaceInfoReport(WorkspaceInfoDashboardBase):
    name: str
    datasetId: Optional[str] = None
    description: str = ""


class WorkspaceInfoUser(BaseModel):
    emailAddress: Optional[str]
    groupUserAccessRight: str
    displayName: Optional[str]
    graphId: str
    principalType: str

    def __hash__(self):
        return hash(self.graphId)


class PowerBiRefreshSchedule(BaseModel):
    days: List[str] = []
    times: List[str] = []
    enabled: bool = False
    localTimeZoneId: str = "UTC"
    notifyOption: str = "MailOnFailure"


class WorkspaceInfoDataflow(BaseModel):
    objectId: str
    name: Optional[str] = None
    description: Optional[str] = None
    configuredBy: Optional[str] = None
    modifiedBy: Optional[str] = None
    modifiedDateTime: Optional[str] = None
    refreshSchedule: Optional[PowerBiRefreshSchedule] = None


class WorkspaceInfo(BaseModel):
    id: str
    name: Optional[str]
    type: Optional[str]
    state: str
    description: Optional[str]
    reports: List[WorkspaceInfoReport] = []
    datasets: List[WorkspaceInfoDataset] = []
    dashboards: List[WorkspaceInfoDashboard] = []
    dataflows: List[WorkspaceInfoDataflow] = []
    users: List[WorkspaceInfoUser] = []


class PowerBiSubscriptionUser(BaseModel):
    emailAddress: str
    displayName: str


class PowerBISubscription(BaseModel):
    id: str
    artifactId: str
    title: Optional[str] = None
    frequency: Optional[str] = None
    endDate: Optional[str] = None
    startDate: Optional[str] = None
    artifactDisplayName: Optional[str] = None
    subArtifactDisplayName: Optional[str] = None
    users: List[PowerBiSubscriptionUser] = []


class SubscriptionsByUserResponse(BaseModel):
    SubscriptionEntities: List[PowerBISubscription]
    continuationUri: Optional[str]


class AccessTokenError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(f"Failed to acquire access token: {message}")


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

        access_token = token.get("access_token")
        if access_token is None:
            raise AccessTokenError(token.get("error_description", "unknown error"))

        return f"Bearer {access_token}"

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

        try:
            return self._call_get(
                url, List[PowerBITile], transform_response=lambda r: r.json()["value"]
            )
        except EntityNotFoundError:
            logger.error(
                f"Unable to find dashboard {dashboard_id}."
                f"Please add the service principal as a viewer to the workspace"
            )
            return []

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

    def get_user_subscriptions(self, user_id: str) -> List[PowerBISubscription]:
        # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/users-get-user-subscriptions-as-admin
        url = f"{self.API_ENDPOINT}/admin/users/{user_id}/subscriptions"

        continuationUri: Optional[str] = url
        subscriptions: List[PowerBISubscription] = []
        while continuationUri:
            try:
                response = self._call_get(continuationUri, SubscriptionsByUserResponse)
            except EntityNotFoundError:
                logger.error(f"Unable to find user {user_id} in workspace.")
                break

            continuationUri = response.continuationUri
            chunk = response.SubscriptionEntities
            if len(chunk) == 0:
                break
            subscriptions += chunk

        return subscriptions

    def get_pages(self, group_id: str, report_id: str) -> List[PowerBIPage]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/reports/get-pages-in-group
        url = f"{self.API_ENDPOINT}/groups/{group_id}/reports/{report_id}/pages"

        try:
            return self._call_get(
                url, List[PowerBIPage], transform_response=lambda r: r.json()["value"]
            )
        except EntityNotFoundError:
            logger.error(
                f"Unable to find report {report_id} in workspace {group_id}. "
                f"Please add the service principal as a viewer to the workspace"
            )
        except Exception as e:
            # Fail gracefully for any other errors
            logger.error(
                f"Failed to get pages from report {report_id} in workspace {group_id}: {e}"
            )

        return []

    def get_refreshes(self, group_id: str, dataset_id: str) -> List[PowerBIRefresh]:
        # https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group
        url = f"{self.API_ENDPOINT}/groups/{group_id}/datasets/{dataset_id}/refreshes"

        try:
            return self._call_get(
                url,
                List[PowerBIRefresh],
                transform_response=lambda r: r.json()["value"],
            )
        except EntityNotFoundError:
            logger.error(
                f"Unable to find dataset {dataset_id} in workspace {group_id}. "
                f"Please add the service principal as a viewer to the workspace"
            )
        except Exception as e:
            # Fail gracefully for any other errors
            logger.error(
                f"Failed to get pages from dataset {dataset_id} in workspace {group_id}: {e}"
            )

        return []

    def export_dataflow(self, group_id: str, dataflow_id: str) -> Optional[dict]:
        # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/dataflows-export-dataflow-as-admin
        url = f"{self.API_ENDPOINT}/admin/dataflows/{dataflow_id}/export"
        try:
            data_sources = self._call_get(
                url,
                dict,
                transform_response=lambda r: r.json(),
            )
            return data_sources
        except Exception as e:
            # Fail gracefully for any other errors
            logger.error(
                f"Failed to get datasource from dataflow {dataflow_id} in workspace {group_id}: {e}"
            )
        return None

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
                timeout=600,  # request timeout 600s
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
                result = requests.get(url, headers=self._headers, timeout=600)
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
            return response.json()["workspaces"]

        scan_id = create_scan()
        scan_success = wait_for_scan_result(scan_id)
        assert scan_success, f"Workspace scan failed, scan_id: {scan_id}"

        # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result
        url = f"{self.API_ENDPOINT}/admin/workspaces/scanResult/{scan_id}"
        workspaces = self._call_get(
            url,
            List[WorkspaceInfo],
            transform_response=transform_scan_result,
        )

        # The scan result will return workspaces without name or type if the workspace does not exist
        return [
            workspace for workspace in workspaces if workspace.name and workspace.type
        ]

    T = TypeVar("T")

    def _call_get(
        self,
        url: str,
        type_: Type[T],
        transform_response: Callable[[requests.Response], Any] = lambda r: r.json(),
    ) -> T:
        try:
            return get_request(
                url,
                self._headers,
                type_,
                transform_response,
            )
        except ApiError as error:
            if error.status_code == 401:
                raise AuthenticationError(error.error_msg) from None
            elif error.status_code == 404:
                raise EntityNotFoundError(error.error_msg) from None
            else:
                raise AssertionError(
                    f"GET {url} failed: {error.status_code}\n{error.error_msg}"
                ) from None
