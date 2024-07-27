from datetime import timedelta
from time import sleep
from typing import Any, Callable, List, Optional, Type, TypeVar
from urllib.parse import quote, urlencode

import requests

from metaphor.common.api_request import ApiError, make_request
from metaphor.common.logger import get_logger
from metaphor.common.utils import start_of_day
from metaphor.power_bi.config import PowerBIRunConfig
from metaphor.power_bi.models import (
    DataflowTransaction,
    GetActivitiesResponse,
    PowerBIActivityEventEntity,
    PowerBIActivityType,
    PowerBIApp,
    PowerBIDashboard,
    PowerBIDataset,
    PowerBIDatasetParameter,
    PowerBIPage,
    PowerBIRefresh,
    PowerBiRefreshSchedule,
    PowerBIReport,
    PowerBISubscription,
    PowerBITile,
    PowerBIWorkspace,
    SubscriptionsByUserResponse,
    WorkspaceInfo,
)

try:
    import msal
except ImportError:
    print("Please install metaphor[power_bi] extra\n")
    raise


logger = get_logger()


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
        self.config = config

    def get_headers(self):
        return {"Authorization": self.retrieve_access_token(self.config)}

    def retrieve_access_token(self, config: PowerBIRunConfig) -> str:
        app = msal.ConfidentialClientApplication(
            config.client_id,
            authority=self.AUTHORITY.format(tenant_id=config.tenant_id),
            client_credential=config.secret,
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

    def get_dataset_parameters(
        self, group_id: str, dataset_id: str
    ) -> List[PowerBIDatasetParameter]:
        # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-parameters-in-group
        url = f"{self.API_ENDPOINT}/groups/{group_id}/datasets/{dataset_id}/parameters"

        try:
            return self._call_get(
                url,
                List[PowerBIDatasetParameter],
                transform_response=lambda r: r.json()["value"],
            )
        except Exception:
            logger.exception(
                f"Unable to get parameters for dataset {dataset_id} in group {group_id}, please add the service principal as a viewer to the workspace"
            )
            return []

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
                f"Failed to get refreshes from dataset {dataset_id} in workspace {group_id}: {e}"
            )

        return []

    def get_refresh_schedule(
        self, group_id: str, dataset_id: str
    ) -> Optional[PowerBiRefreshSchedule]:
        # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-schedule
        url = f"{self.API_ENDPOINT}/groups/{group_id}/datasets/{dataset_id}/refreshSchedule"

        try:
            return self._call_get(
                url,
                PowerBiRefreshSchedule,
                transform_response=lambda r: r.json(),
            )
        except EntityNotFoundError:
            logger.error(
                f"Unable to find dataset {dataset_id} in workspace {group_id}. "
                f"Please add the service principal as a viewer to the workspace"
            )
        except Exception as e:
            # Fail gracefully for any other errors
            logger.error(
                f"Failed to get refresh schedule from dataset {dataset_id} in workspace {group_id}: {e}"
            )

        return None

    def get_direct_query_refresh_schedule(
        self, group_id: str, dataset_id: str
    ) -> Optional[PowerBiRefreshSchedule]:
        # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-direct-query-refresh-schedule-in-group
        url = f"{self.API_ENDPOINT}/groups/{group_id}/datasets/{dataset_id}/directQueryRefreshSchedule"

        try:
            return self._call_get(
                url,
                PowerBiRefreshSchedule,
                transform_response=lambda r: r.json(),
            )
        except EntityNotFoundError:
            logger.error(
                f"Unable to find dataset {dataset_id} in workspace {group_id}. "
                f"Please add the service principal as a viewer to the workspace"
            )
        except Exception as e:
            # Fail gracefully for any other errors
            logger.error(
                f"Failed to get refresh schedule from dataset {dataset_id} in workspace {group_id}: {e}"
            )

        return None

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

    def get_dataflow_transactions(
        self, group_id: str, dataflow_id: str
    ) -> List[DataflowTransaction]:
        url = f"{self.API_ENDPOINT}/groups/{group_id}/dataflows/{dataflow_id}/transactions"
        try:
            data_sources = self._call_get(
                url,
                List[DataflowTransaction],
                transform_response=lambda r: r.json()["value"],
            )
            return data_sources
        except Exception as e:
            # Fail gracefully for any other errors
            logger.error(
                f"Failed to get transactions from dataflow {dataflow_id} in workspace {group_id}: {e}"
            )
        return []

    def get_workspace_info(self, workspace_ids: List[str]) -> List[WorkspaceInfo]:
        def create_scan() -> str:
            # https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info
            url = f"{self.API_ENDPOINT}/admin/workspaces/getInfo"
            request_body = {"workspaces": workspace_ids}
            result = requests.post(
                url,
                headers=self.get_headers(),
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
                result = requests.get(url, headers=self.get_headers(), timeout=600)
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

    def _get_activities(self, url: str) -> List[PowerBIActivityEventEntity]:
        continuationUri: Optional[str] = url
        activities: List[PowerBIActivityEventEntity] = []

        while continuationUri:
            try:
                response = self._call_get(
                    continuationUri,
                    GetActivitiesResponse,
                )
            except EntityNotFoundError:
                logger.error(f"HTTP 404 error, url: {continuationUri}")
                break

            continuationUri = response.continuationUri
            chunk = response.activityEventEntities
            if len(chunk) == 0:
                break

            def is_view_activity(activity: PowerBIActivityEventEntity) -> bool:
                if activity.Activity in (
                    PowerBIActivityType.view_report.value,
                    PowerBIActivityType.view_dashboard,
                ):
                    return True
                return False

            activities.extend(filter(is_view_activity, chunk))
        return activities

    def get_activities(
        self,
        lookback_days: int = 1,
    ) -> List[PowerBIActivityEventEntity]:
        # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/get-activity-events
        endpoint = f"{self.API_ENDPOINT}/admin/activityevents"

        activities: List[PowerBIActivityEventEntity] = []

        start_date = start_of_day(lookback_days)
        end_date = start_date + timedelta(days=1) - timedelta(milliseconds=1)
        for _ in range(lookback_days + 1):
            params = {
                "startDateTime": f"'{start_date.isoformat(timespec='milliseconds').replace('+00:00', 'Z')}'",
                "endDateTime": f"'{end_date.isoformat(timespec='milliseconds').replace('+00:00', 'Z')}'",
            }

            url = f"{endpoint}?{urlencode(params, quote_via=quote)}"
            activities.extend(self._get_activities(url))

            start_date += timedelta(days=1)
            end_date += timedelta(days=1)

        return activities

    T = TypeVar("T")

    def _call_get(
        self,
        url: str,
        type_: Type[T],
        transform_response: Callable[[requests.Response], Any] = lambda r: r.json(),
    ) -> T:
        try:
            return make_request(
                url,
                self.get_headers(),
                type_,
                transform_response,
            )
        except ApiError as error:
            if error.status_code == 401:
                raise AuthenticationError(error.body) from None
            elif error.status_code == 404:
                raise EntityNotFoundError(error.body) from None
            else:
                raise AssertionError(
                    f"GET {url} failed: {error.status_code}\n{error.body}"
                ) from None
