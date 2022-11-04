import re
from datetime import datetime, timezone
from typing import Collection, Dict, List, Optional
from urllib import parse

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import EntityId
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import chunks, unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    Chart,
    ChartType,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    EntityType,
)
from metaphor.models.metadata_change_event import PowerBIApp as PbiApp
from metaphor.models.metadata_change_event import PowerBIColumn as PbiColumn
from metaphor.models.metadata_change_event import PowerBIDashboardType
from metaphor.models.metadata_change_event import (
    PowerBIDataset as VirtualViewPowerBIDataset,
)
from metaphor.models.metadata_change_event import PowerBIDatasetTable, PowerBIInfo
from metaphor.models.metadata_change_event import PowerBIMeasure as PbiMeasure
from metaphor.models.metadata_change_event import PowerBIWorkspace as PbiWorkspace
from metaphor.models.metadata_change_event import (
    SourceInfo,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from metaphor.power_bi.config import PowerBIRunConfig
from metaphor.power_bi.power_bi_client import (
    PowerBIApp,
    PowerBIClient,
    PowerBIPage,
    PowerBIRefresh,
    PowerBIRefreshStatus,
    PowerBITile,
    WorkspaceInfo,
)
from metaphor.power_bi.power_query_parser import PowerQueryParser

logger = get_logger()


class PowerBIExtractor(BaseExtractor):
    """Power BI metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "PowerBIExtractor":
        return PowerBIExtractor(PowerBIRunConfig.from_yaml_file(config_file))

    def __init__(self, config: PowerBIRunConfig):
        super().__init__(config, "Power BI metadata crawler", Platform.POWER_BI)
        self._tenant_id = config.tenant_id
        self._workspaces = config.workspaces

        self._client = PowerBIClient(config)
        self._dashboards: Dict[str, Dashboard] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._snowflake_account = config.snowflake_account

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from Power BI tenant ID: {self._tenant_id}")

        if len(self._workspaces) == 0:
            self._workspaces = [w.id for w in self._client.get_groups()]

        logger.info(f"Process {len(self._workspaces)} workspaces: {self._workspaces}")

        apps = self._client.get_apps()
        app_map = {app.id: app for app in apps}

        for workspace_ids in chunks(
            self._workspaces, PowerBIClient.MAX_WORKSPACES_PER_SCAN
        ):
            for workspace in self._client.get_workspace_info(workspace_ids):
                logger.info(
                    f"Fetching metadata from Power BI workspace ID: {workspace.id}"
                )

                try:
                    self.map_wi_datasets_to_virtual_views(workspace)
                    self.map_wi_reports_to_dashboard(workspace, app_map)
                    self.map_wi_dashboards_to_dashboard(workspace, app_map)
                    self.dedupe_app_version_dashboards()
                except Exception as e:
                    logger.exception(e)

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._virtual_views.values())
        entities.extend(self._dashboards.values())

        return entities

    def map_wi_datasets_to_virtual_views(self, workspace: WorkspaceInfo) -> None:

        dataset_map = {d.id: d for d in self._client.get_datasets(workspace.id)}

        for wds in workspace.datasets:
            expressions = []
            tables = []
            for table in wds.tables:
                tables.append(
                    PowerBIDatasetTable(
                        columns=[
                            PbiColumn(field=c.name, type=c.dataType)
                            for c in table.columns
                        ],
                        measures=[
                            PbiMeasure(
                                field=m.name,
                                expression=m.expression,
                                description=m.description,
                            )
                            for m in table.measures
                        ],
                        name=table.name,
                    )
                )

                for source in table.source:
                    expressions.append(source["expression"])

            source_datasets = []
            for expression in expressions:
                try:
                    source_datasets.extend(
                        PowerQueryParser.parse_source_datasets(
                            expression, self._snowflake_account
                        )
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to parse expression: {expression} for dataset {wds.id}"
                    )
                    logger.exception(e)

            ds = dataset_map.get(wds.id, None)
            if ds is None:
                logger.warn(f"Skipping invalid dataset {wds.id}")
                continue

            last_refreshed = None
            if ds.isRefreshable:
                try:
                    # Log errors instead of throwing exceptions as this API call requires extra permissions
                    refreshes = self._client.get_refreshes(workspace.id, wds.id)
                    last_refreshed = self._find_last_completed_refresh(refreshes)
                except Exception as e:
                    logger.exception(e)

            virtual_view = VirtualView(
                logical_id=VirtualViewLogicalID(
                    name=wds.id, type=VirtualViewType.POWER_BI_DATASET
                ),
                power_bi_dataset=VirtualViewPowerBIDataset(
                    tables=tables,
                    name=wds.name,
                    url=ds.webUrl,
                    source_datasets=unique_list([str(d) for d in source_datasets]),
                    description=wds.description,
                    workspace=PbiWorkspace(id=workspace.id, name=workspace.name),
                    last_refreshed=last_refreshed,
                ),
            )

            self._virtual_views[wds.id] = virtual_view

    def map_wi_reports_to_dashboard(
        self, workspace: WorkspaceInfo, app_map: Dict[str, PowerBIApp]
    ) -> None:

        report_map = {r.id: r for r in self._client.get_reports(workspace.id)}

        for wi_report in workspace.reports:
            if wi_report.datasetId is None:
                logger.warn(f"Skipping report without datasetId: {wi_report.id}")
                continue

            upstream_id = str(
                EntityId(
                    EntityType.VIRTUAL_VIEW,
                    self._virtual_views[wi_report.datasetId].logical_id,
                )
            )

            report = report_map.get(wi_report.id, None)
            if report is None:
                logger.warn(f"Skipping invalid report {wi_report.id}")
                continue

            pbi_info = self._make_power_bi_info(
                PowerBIDashboardType.REPORT, workspace, wi_report.appId, app_map
            )

            # The "app" version of report doesn't have pages
            charts = None
            if wi_report.appId is None:
                # Log errors instead of throwing exceptions as this API call requires extra permissions
                try:
                    pages = self._client.get_pages(workspace.id, wi_report.id)
                    charts = self.transform_pages_to_charts(pages)
                except Exception as e:
                    logger.exception(e)

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=wi_report.id,
                    platform=DashboardPlatform.POWER_BI,
                ),
                dashboard_info=DashboardInfo(
                    description=wi_report.description,
                    title=wi_report.name,
                    power_bi=pbi_info,
                    charts=charts,
                ),
                source_info=SourceInfo(
                    main_url=report.webUrl,
                ),
                upstream=DashboardUpstream(source_virtual_views=[upstream_id]),
            )
            self._dashboards[wi_report.id] = dashboard

    def map_wi_dashboards_to_dashboard(
        self, workspace: WorkspaceInfo, app_map: Dict[str, PowerBIApp]
    ) -> None:

        dashboard_map = {d.id: d for d in self._client.get_dashboards(workspace.id)}

        for wi_dashboard in workspace.dashboards:
            tiles = self._client.get_tiles(wi_dashboard.id)
            upstream = []
            for tile in tiles:
                dataset_id = tile.datasetId

                # skip tile not depends on a dataset
                if dataset_id == "":
                    continue

                upstream.append(
                    str(
                        EntityId(
                            EntityType.VIRTUAL_VIEW,
                            self._virtual_views[dataset_id].logical_id,
                        )
                    )
                )

            pbi_dashboard = dashboard_map.get(wi_dashboard.id, None)
            if pbi_dashboard is None:
                logger.warn(f"Skipping invalid dashboard {wi_dashboard.id}")
                continue

            pbi_info = self._make_power_bi_info(
                PowerBIDashboardType.DASHBOARD, workspace, wi_dashboard.appId, app_map
            )

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=wi_dashboard.id,
                    platform=DashboardPlatform.POWER_BI,
                ),
                dashboard_info=DashboardInfo(
                    title=wi_dashboard.displayName,
                    charts=self.transform_tiles_to_charts(tiles),
                    power_bi=pbi_info,
                ),
                source_info=SourceInfo(
                    main_url=pbi_dashboard.webUrl,
                ),
                upstream=DashboardUpstream(source_virtual_views=unique_list(upstream)),
            )
            self._dashboards[wi_dashboard.id] = dashboard

    def dedupe_app_version_dashboards(self):
        for dashboard_id in list(self._dashboards.keys()):
            dashboard = self._dashboards.get(dashboard_id)
            if dashboard.dashboard_info.power_bi.app is None:
                # Non-app dashboard
                continue

            app_url = dashboard.source_info.main_url
            original_dashboard_id = self._get_dashboard_id_from_url(
                dashboard.source_info.main_url
            )
            original_dashboard = self._dashboards.get(original_dashboard_id)
            if original_dashboard is None:
                # Cannot not found corresponding non-app dashboard
                logger.warning(
                    f"Non-app version dashboard not found, id: {original_dashboard_id}"
                )
                continue

            original_dashboard.dashboard_info.power_bi.app = (
                dashboard.dashboard_info.power_bi.app
            )

            # replace source url with app_url if possible
            original_dashboard.source_info.main_url = (
                app_url
                if app_url is not None
                else original_dashboard.source_info.main_url
            )
            del self._dashboards[dashboard_id]

    def _make_power_bi_info(
        self,
        type: PowerBIDashboardType,
        workspace: WorkspaceInfo,
        app_id: Optional[str],
        app_map: Dict[str, PowerBIApp],
    ) -> PowerBIInfo:
        pbi_info = PowerBIInfo(
            power_bi_dashboard_type=type,
            workspace=PbiWorkspace(id=workspace.id, name=workspace.name),
        )

        if app_id is not None:
            app = app_map.get(app_id)
            if app is not None:
                pbi_info.app = PbiApp(id=app.id, name=app.name)

        return pbi_info

    def _find_last_completed_refresh(
        self, refreshes: List[PowerBIRefresh]
    ) -> Optional[datetime]:
        try:
            # Assume refreshes are already sorted in reversed chronological order
            # Empty endTime means still in progress
            refresh = next(
                r
                for r in refreshes
                if r.status == PowerBIRefreshStatus.COMPLETED and r.endTime != ""
            )
        except StopIteration:
            return None

        # Make time ISO-compliant by stripping off non-zero padded ms and "Z"
        iso_time = ".".join(refresh.endTime.split(".")[0:-1])
        return datetime.fromisoformat(iso_time).replace(tzinfo=timezone.utc)

    def _get_dashboard_id_from_url(self, url: str) -> Optional[str]:
        path = parse.urlparse(url).path
        pattern = re.compile(r"apps/([^/]+)/(reports|dashboards)/([^/]+)")
        match = pattern.search(path)
        if match and len(match.groups()) == 3:
            return match.group(3)
        return None

    @staticmethod
    def transform_tiles_to_charts(tiles: List[PowerBITile]) -> List[Chart]:
        return [
            Chart(title=t.title, url=t.embedUrl, chart_type=ChartType.OTHER)
            for t in tiles
        ]

    @staticmethod
    def transform_pages_to_charts(pages: List[PowerBIPage]) -> List[Chart]:
        return [Chart(title=p.displayName, chart_type=ChartType.OTHER) for p in pages]
