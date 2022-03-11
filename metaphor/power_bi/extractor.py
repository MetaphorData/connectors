import re
from datetime import datetime
from time import sleep
from typing import Any, Callable, Collection, Dict, List, Optional, Type, TypeVar

import requests

from metaphor.common.entity_id import EntityId, dataset_fullname, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.power_bi.config import PowerBIRunConfig

try:
    import msal
except ImportError:
    print("Please install metaphor[power_bi] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    DataPlatform,
    EntityType,
    PowerBIDashboardType,
)
from pydantic import BaseModel, parse_obj_as

from metaphor.common.extractor import BaseExtractor

logger = get_logger(__name__)


class PowerBIDataSource(BaseModel):
    datasourceType: str
    datasourceId: str
    connectionDetails: Any
    gatewayId: str


class PowerBIDataset(BaseModel):
    id: str
    name: str
    createdDate: Optional[datetime] = None
    configuredBy: str
    targetStorageMode: str
    webUrl: str = ""


class PowerBIDashboard(BaseModel):
    id: str
    displayName: str
    appId: str = ""
    webUrl: str = ""


class PowerBIWorkspace(BaseModel):
    id: str
    name: str
    isReadOnly: bool
    type: str


class PowerBIReport(BaseModel):
    id: str
    name: str
    datasetId: str
    reportType: str
    webUrl: str


class PowerBITile(BaseModel):
    id: str
    title: str
    datasetId: str = ""
    reportId: str = ""
    embedUrl: str


class PowerBITableColumn(BaseModel):
    name: str
    datatype: Optional[str] = None
    isHidden: bool
    columnType: str


class PowerBITable(BaseModel):
    name: str
    columns: List[PowerBITableColumn]
    measures: List[Any]
    source: List[Any]


class WorkspaceInfoDataset(BaseModel):
    configuredBy: str
    id: str
    name: str
    tables: List[PowerBITable]

    description: str = ""
    ContentProviderType: str = ""
    CreatedDate: str = ""

    upstreamDataflows: Any
    upstreamDatasets: Any


class WorkspaceInfoDashboard(BaseModel):
    displayName: str
    id: str


class WorkspaceInfoReport(BaseModel):
    id: str
    name: str
    datasetId: str
    description: str = ""


class WorkspaceInfo(BaseModel):
    id: str
    name: str
    type: str
    state: str
    reports: List[WorkspaceInfoReport]
    datasets: List[WorkspaceInfoDataset]
    dashboards: List[WorkspaceInfoDashboard]


class PowerBIClient:
    AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
    SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]
    API_ENDPOINT = "https://api.powerbi.com/v1.0/myorg"

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
        url = f"{self.API_ENDPOINT}/groups"
        return self._call_get(
            url, List[PowerBIWorkspace], transform_response=lambda r: r.json()["value"]
        )

    def get_tiles(self, group_id: str, dashboard_id: str) -> List[PowerBITile]:
        url = f"{self.API_ENDPOINT}/groups/{group_id}/dashboards/{dashboard_id}/tiles"
        return self._call_get(
            url, List[PowerBITile], transform_response=lambda r: r.json()["value"]
        )

    def get_dataset(self, group_id: str, dataset_id: str) -> PowerBIDataset:
        url = f"{self.API_ENDPOINT}/groups/{group_id}/datasets/{dataset_id}"
        return self._call_get(url, PowerBIDataset)

    def get_report(self, group_id: str, report_id: str) -> PowerBIReport:
        url = f"{self.API_ENDPOINT}/groups/{group_id}/reports/{report_id}"
        return self._call_get(url, PowerBIReport)

    def get_datasource(
        self, group_id: str, dashboard_id: str
    ) -> List[PowerBIDataSource]:
        url = (
            f"{self.API_ENDPOINT}/groups/{group_id}/datasets/{dashboard_id}/datasources"
        )
        return self._call_get(url, List[PowerBIDataSource])

    def get_workspace_info(self, workspace_id: str) -> WorkspaceInfo:
        def create_scan() -> str:
            url = f"{self.API_ENDPOINT}/admin/workspaces/getInfo"
            request_body = {"workspaces": [workspace_id]}
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
                f"workspace_id: {workspace_id}, "
                f"response: [{result.status_code}] {result.content.decode()}"
            )

            scan_id = result.json()["id"]
            logger.info(f"Create a scan, id: {scan_id}")

            return result.json()["id"]

        def wait_for_scan_result(scan_id: str, max_timeout_in_secs: int = 30) -> bool:
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

        scan_id = create_scan()
        scan_success = wait_for_scan_result(scan_id)
        assert scan_success, f"Workspace scan failed, scan_id: {scan_id}"

        # Since we only request one workspace per scan, result should contain one workspace
        def get_first_workspace(response: requests.Response) -> Any:
            return response.json()["workspaces"][0]

        url = f"{self.API_ENDPOINT}/admin/workspaces/scanResult/{scan_id}"
        return self._call_get(url, WorkspaceInfo, get_first_workspace)

    T = TypeVar("T")

    def _call_get(
        self,
        url: str,
        type_: Type[T],
        transform_response: Callable[[requests.Response], Any] = lambda r: r.json(),
    ) -> T:
        result = requests.get(url, headers=self._headers)
        assert result.status_code == 200, f"GET {url} failed, {result.content.decode()}"
        return parse_obj_as(type_, transform_response(result))


class PowerBIExtractor(BaseExtractor):
    """Power BI metadata extractor"""

    @staticmethod
    def config_class():
        return PowerBIRunConfig

    def __init__(self):
        self._dashboards: Dict[str, Dashboard] = {}

    async def extract(self, config: PowerBIRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, PowerBIExtractor.config_class())
        logger.info(f"Fetching metadata from Power BI tenant ID: {config.tenant_id}")

        self.client = PowerBIClient(config)

        if len(config.workspaces) == 0:
            config.workspaces = [w.id for w in self.client.get_groups()]

        for workspace_id in config.workspaces:
            logger.info(f"Fetching metadata from Power BI workspace ID: {workspace_id}")

            info = self.client.get_workspace_info(workspace_id)
            self.map_wi_datasets_to_dashboard(workspace_id, info.datasets)
            self.map_wi_reports_to_dashboard(workspace_id, info.reports)
            self.map_wi_dashboards_to_dashboard(workspace_id, info.dashboards)

        return self._dashboards.values()

    def map_wi_datasets_to_dashboard(
        self, workspace_id: str, wi_datasets: List[WorkspaceInfoDataset]
    ) -> None:
        for wds in wi_datasets:
            source_datasets = set()
            for table in wds.tables:
                for source in table.source:
                    power_query_text = source["expression"]

                    if "Value.NativeQuery(" in power_query_text:
                        logger.warning(
                            f"Skip {table.name}, because it is created from Custom SQL"
                        )
                        continue
                    try:
                        entity_id = str(
                            PowerBIExtractor.parse_power_query(power_query_text)
                        )
                        source_datasets.add(entity_id)
                    except AssertionError:
                        logger.warning(
                            f"Parsing upstream fail, exp: {power_query_text}"
                        )
            ds = self.client.get_dataset(workspace_id, wds.id)

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=wds.id, platform=DashboardPlatform.POWER_BI
                ),
                dashboard_info=DashboardInfo(
                    created_at=ds.createdDate,
                    description=wds.description,
                    power_bi_dashboard_type=PowerBIDashboardType.DATASET,
                    title=wds.name,
                    url=ds.webUrl,
                ),
                upstream=DashboardUpstream(source_datasets=list(source_datasets)),
            )

            self._dashboards[wds.id] = dashboard

    def map_wi_reports_to_dashboard(
        self, workspace_id: str, wi_reports: List[WorkspaceInfoReport]
    ) -> None:
        for wi_report in wi_reports:
            upstream_id = str(
                EntityId(
                    EntityType.DASHBOARD,
                    self._dashboards[wi_report.datasetId].logical_id,
                )
            )
            report = self.client.get_report(workspace_id, wi_report.id)
            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=wi_report.id,
                    platform=DashboardPlatform.POWER_BI,
                ),
                dashboard_info=DashboardInfo(
                    description=wi_report.description,
                    power_bi_dashboard_type=PowerBIDashboardType.REPORT,
                    title=wi_report.name,
                    url=report.webUrl,
                ),
                upstream=DashboardUpstream(source_datasets=[upstream_id]),
            )
            self._dashboards[wi_report.id] = dashboard

    def map_wi_dashboards_to_dashboard(
        self, workspace_id: str, wi_dashboards: List[WorkspaceInfoDashboard]
    ) -> None:
        for wi_dashboard in wi_dashboards:
            tiles = self.client.get_tiles(workspace_id, wi_dashboard.id)
            upstream = set()
            for tile in tiles:
                upstream.add(
                    str(
                        EntityId(
                            EntityType.DASHBOARD,
                            self._dashboards[tile.reportId].logical_id,
                        )
                    )
                )
            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=wi_dashboard.id,
                    platform=DashboardPlatform.POWER_BI,
                ),
                dashboard_info=DashboardInfo(
                    title=wi_dashboard.displayName,
                    charts=self.transform_tiles_to_charts(tiles),
                    power_bi_dashboard_type=PowerBIDashboardType.DASHBOARD,
                ),
                upstream=DashboardUpstream(source_datasets=list(upstream)),
            )
            self._dashboards[wi_dashboard.id] = dashboard

    @staticmethod
    def parse_power_query(expression: str) -> EntityId:
        lines = expression.split("\n")
        platform_pattern = re.compile(r"Source = (\w+).")
        match = platform_pattern.search(lines[1])
        assert match, "Can't parse platform from power query expression."
        platform_str = match.group(1)

        field_pattern = re.compile(r'{\[Name="([\w\-]+)"(.*)\]}')

        def get_field(text: str) -> str:
            match = field_pattern.search(text)
            assert match, "Can't parse field from power query expression"
            return match.group(1)

        account = None
        if platform_str == "AmazonRedshift":
            platform = DataPlatform.REDSHIFT
            db_pattern = re.compile(r"Source = (\w+).Database\((.*)\),$")
            match = db_pattern.search(lines[1])
            assert (
                match
            ), "Can't parse AmazonRedshift database from power query expression"

            db = match.group(2).split(",")[1].replace('"', "")
            schema = get_field(lines[2])
            table = get_field(lines[3])
        elif platform_str == "Snowflake":
            platform = DataPlatform.SNOWFLAKE
            account_pattern = re.compile(r'Snowflake.Databases\("([\w\-\.]+)"')

            # remove trailing snowflakecomputing.com
            match = account_pattern.search(lines[1])
            assert match, "Can't parse Snowflake account from power query expression"

            account = ".".join(match.group(1).split(".")[:-2])
            db = get_field(lines[2])
            schema = get_field(lines[3])
            table = get_field(lines[4])
        elif platform_str == "GoogleBigQuery":
            platform = DataPlatform.BIGQUERY
            db = get_field(lines[2])
            schema = get_field(lines[3])
            table = get_field(lines[4])

        return to_dataset_entity_id(
            dataset_fullname(db, schema, table), platform, account
        )

    @staticmethod
    def transform_tiles_to_charts(tiles: List[PowerBITile]) -> List[Chart]:
        return [Chart(title=t.title, url=t.embedUrl) for t in tiles]
