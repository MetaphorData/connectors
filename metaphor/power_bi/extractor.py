import json
from datetime import datetime, timezone
from typing import Collection, Dict, List, Optional

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    EntityId,
    to_dashboard_entity_id_from_logical_id,
    to_person_entity_id,
    to_pipeline_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import chunks, is_email, safe_parse_ISO8601, unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetStructure,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardType,
    EntityType,
    EntityUpstream,
    GroupUserAccessRight,
    Hierarchy,
    HierarchyInfo,
    HierarchyLogicalID,
    HierarchyType,
    Pipeline,
    PipelineInfo,
    PipelineLogicalID,
    PipelineMapping,
    PipelineType,
    PowerBIDashboardType,
    PowerBIDataflow,
)
from metaphor.models.metadata_change_event import (
    PowerBIDataset as VirtualViewPowerBIDataset,
)
from metaphor.models.metadata_change_event import (
    PowerBIDatasetParameter,
    PowerBIDatasource,
    PowerBIRefreshSchedule,
)
from metaphor.models.metadata_change_event import PowerBISubscription as Subscription
from metaphor.models.metadata_change_event import (
    PowerBISubscriptionUser as SubscriptionUser,
)
from metaphor.models.metadata_change_event import PowerBIWorkspace as PbiWorkspace
from metaphor.models.metadata_change_event import (
    PowerBIWorkspaceUser,
    SourceInfo,
    UserActivity,
    UserActivityActorInfo,
    UserActivitySource,
    UserActivityType,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from metaphor.power_bi.config import PowerBIRunConfig
from metaphor.power_bi.graph_api_client import GraphApiClient
from metaphor.power_bi.models import (
    PowerBIApp,
    PowerBIDashboard,
    PowerBIDataset,
    PowerBIReport,
    PowerBISubscription,
    PowerBiSubscriptionUser,
    WorkspaceInfo,
    WorkspaceInfoDataset,
)
from metaphor.power_bi.power_bi_client import PowerBIClient
from metaphor.power_bi.power_bi_utils import (
    extract_refresh_schedule,
    find_last_completed_refresh,
    find_refresh_time_from_transaction,
    get_dashboard_id_from_url,
    get_pbi_dataset_tables,
    get_workspace_hierarchy,
    make_power_bi_info,
    transform_pages_to_charts,
    transform_tiles_to_charts,
)

logger = get_logger()


class PowerBIExtractor(BaseExtractor):
    """Power BI metadata extractor"""

    _description = "Power BI metadata crawler"
    _platform = Platform.POWER_BI

    @staticmethod
    def from_config_file(config_file: str) -> "PowerBIExtractor":
        return PowerBIExtractor(PowerBIRunConfig.from_yaml_file(config_file))

    def __init__(self, config: PowerBIRunConfig):
        super().__init__(config)
        self._config = config
        self._tenant_id = config.tenant_id
        self._workspaces = config.workspaces

        self._client = PowerBIClient(self._config)
        self._graph_client = GraphApiClient(self._config)

        self._dashboards: Dict[str, Dashboard] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._pipelines: Dict[str, Pipeline] = {}
        self._dataflow_sources: Dict[str, List[EntityId]] = {}
        self._hierarchies: List[Hierarchy] = []
        self._snowflake_account = config.snowflake_account

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from Power BI tenant ID: {self._tenant_id}")

        dataset_map = {d.id: d for d in self._client.get_datasets()}
        dashboard_map = {d.id: d for d in self._client.get_dashboards()}
        report_map = {r.id: r for r in self._client.get_reports()}
        app_map = {app.id: app for app in self._client.get_apps()}

        if len(self._workspaces) == 0:
            self._workspaces = [w.id for w in self._client.get_groups()]

        logger.info(
            f"Processing {len(self._workspaces)} workspaces: {self._workspaces}"
        )

        workspaces: List[WorkspaceInfo] = []

        for workspace_ids in chunks(
            self._workspaces, PowerBIClient.MAX_WORKSPACES_PER_SCAN
        ):
            workspaces.extend(self._client.get_workspace_info(workspace_ids))

        for workspace in workspaces:
            self.map_wi_dataflow_to_pipeline(workspace)
            self.map_workspace_to_hierarchy(workspace)

        # As there may be cross-workspace reference in dashboards & reports,
        # we must process the datasets across all workspaces first
        for workspace in workspaces:
            await self.map_wi_datasets_to_virtual_views(workspace, dataset_map)

        for workspace in workspaces:
            await self.map_wi_reports_to_dashboard(workspace, report_map, app_map)
            await self.map_wi_dashboards_to_dashboard(workspace, dashboard_map, app_map)

        self.extract_subscriptions(workspaces)

        self.dedupe_app_version_dashboards()

        user_activities = self.extract_activities()

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._virtual_views.values())
        entities.extend(self._dashboards.values())
        entities.extend(self._pipelines.values())
        entities.extend(self._hierarchies)
        entities.extend(user_activities)

        return entities

    def map_workspace_to_hierarchy(self, workspace: WorkspaceInfo) -> None:
        workspace_id = workspace.id

        def get_access_right(access_right_str: str) -> Optional[GroupUserAccessRight]:
            if access_right_str in (
                GroupUserAccessRight.ADMIN.value,
                GroupUserAccessRight.MEMBER.value,
                GroupUserAccessRight.VIEWER.value,
            ):
                return GroupUserAccessRight(access_right_str)
            return None

        users = [
            PowerBIWorkspaceUser(
                email_address=user.emailAddress,
                display_name=user.displayName,
                group_user_access_right=get_access_right(user.groupUserAccessRight),
            )
            for user in workspace.users or []
            if user.emailAddress  # Filter out group or service principal
        ]

        pbi_workspace = PbiWorkspace(
            name=workspace.name,
            url=f"https://app.powerbi.com/groups/{workspace_id}",
            users=users,
        )
        hierarchy_info = HierarchyInfo(
            description=workspace.description,
            type=HierarchyType.POWER_BI_WORKSPACE,
            name=workspace.name,
            power_bi_workspace=pbi_workspace,
        )
        self._hierarchies.append(
            Hierarchy(
                logical_id=HierarchyLogicalID(
                    path=[VirtualViewType.POWER_BI_DATASET.value, workspace_id]
                ),
                hierarchy_info=hierarchy_info,
            )
        )
        self._hierarchies.append(
            Hierarchy(
                logical_id=HierarchyLogicalID(
                    path=[DashboardPlatform.POWER_BI.value, workspace_id]
                ),
                hierarchy_info=hierarchy_info,
            )
        )
        self._hierarchies.append(
            Hierarchy(
                logical_id=HierarchyLogicalID(
                    path=[PipelineType.POWER_BI_DATAFLOW.value, workspace_id]
                ),
                hierarchy_info=hierarchy_info,
            )
        )

    def map_wi_dataflow_to_pipeline(self, workspace: WorkspaceInfo) -> None:
        for wdf in workspace.dataflows:
            data_flow_id = wdf.objectId

            dataflow: Optional[dict] = self._client.export_dataflow(
                workspace.id, data_flow_id
            )

            document_str: Optional[str] = None
            if dataflow is not None:
                document_str = dataflow.get("pbi:mashup", {}).get("document")

            transactions = self._client.get_dataflow_transactions(
                workspace.id, data_flow_id
            )

            pipeline = Pipeline(
                logical_id=PipelineLogicalID(
                    name=data_flow_id,
                    type=PipelineType.POWER_BI_DATAFLOW,
                ),
                power_bi_dataflow=PowerBIDataflow(
                    configured_by=wdf.configuredBy,
                    description=wdf.description,
                    document=document_str,
                    modified_by=wdf.configuredBy,
                    modified_date_time=safe_parse_ISO8601(wdf.modifiedDateTime),
                    content=json.dumps(dataflow) if dataflow else None,
                    name=wdf.name,
                    refresh_schedule=(
                        PowerBIRefreshSchedule(
                            days=wdf.refreshSchedule.days,
                            times=wdf.refreshSchedule.times,
                            enabled=wdf.refreshSchedule.enabled,
                            local_time_zone_id=wdf.refreshSchedule.localTimeZoneId,
                            notify_option=wdf.refreshSchedule.notifyOption,
                        )
                        if wdf.refreshSchedule
                        else None
                    ),
                    dataflow_url=f"https://app.powerbi.com/groups/{workspace.id}/dataflows/{data_flow_id}",
                    workspace_id=workspace.id,
                    last_refreshed=find_refresh_time_from_transaction(transactions),
                ),
            )

            self._pipelines[data_flow_id] = pipeline

    def _extract_pipeline_info(
        self, wds: WorkspaceInfoDataset, virtual_view: VirtualView
    ) -> None:
        if not wds.upstreamDataflows:
            return

        # TODO(sc-21025): to support multiple pipelines
        first_pipeline_entity_id: Optional[str] = None

        pipeline_mappings: List[PipelineMapping] = []

        for df in wds.upstreamDataflows:
            dataflow = self._pipelines.get(df.targetDataflowId)

            if dataflow is None:
                logger.warning(f"Can't found dataflow: {df.targetDataflowId}")
                continue

            pipeline_entity_id = str(
                to_pipeline_entity_id_from_logical_id(dataflow.logical_id)
            )

            first_pipeline_entity_id = first_pipeline_entity_id or pipeline_entity_id

            # Get supported source entity ids
            supported_source_entities = (
                self._dataflow_sources.get(df.targetDataflowId) or []
            )

            for entity_id in supported_source_entities:
                pipeline_mappings.append(
                    PipelineMapping(
                        is_virtual=False,
                        pipeline_entity_id=pipeline_entity_id,
                        source_entity_id=str(entity_id),
                    )
                )

            # Set virtual upstream entity if there is no supported source entity
            if not supported_source_entities:
                pipeline_mappings.append(
                    PipelineMapping(
                        is_virtual=True, pipeline_entity_id=pipeline_entity_id
                    )
                )

        source_entity_ids = unique_list(
            [
                str(entity_id)
                for df in wds.upstreamDataflows
                for entity_id in (self._dataflow_sources.get(df.targetDataflowId) or [])
            ]
        )

        if source_entity_ids or first_pipeline_entity_id:
            virtual_view.entity_upstream = EntityUpstream(
                source_entities=source_entity_ids or None,
            )
            virtual_view.power_bi_dataset.source_datasets = source_entity_ids or None

        if pipeline_mappings:
            virtual_view.pipeline_info = PipelineInfo(
                pipeline_mapping=pipeline_mappings
            )

    async def map_wi_datasets_to_virtual_views(
        self, workspace: WorkspaceInfo, dataset_map: Dict[str, PowerBIDataset]
    ) -> None:
        for wds in workspace.datasets:
            ds = dataset_map.get(wds.id, None)
            if ds is None:
                logger.warning(f"Skipping invalid dataset {wds.id}")
                continue

            last_refreshed = None
            if ds.isRefreshable:
                refreshes = self._client.get_refreshes(workspace.id, wds.id)
                last_refreshed = find_last_completed_refresh(refreshes)

            refresh_schedule = extract_refresh_schedule(
                self._client, workspace.id, wds.id
            )

            parameters = [
                PowerBIDatasetParameter(
                    name=p.name,
                    type=p.type,
                    value=p.currentValue,
                    is_required=p.isRequired,
                )
                for p in self._client.get_dataset_parameters(workspace.id, wds.id)
            ]

            data_sources = [
                PowerBIDatasource(
                    type=ds.datasourceType,
                    datasource_id=ds.datasourceId,
                    gateway_id=ds.gatewayId,
                    kind=(ds.connectionDetails or {}).get("kind", None),
                    url=(ds.connectionDetails or {}).get("url", None),
                    path=(ds.connectionDetails or {}).get("path", None),
                    server=(ds.connectionDetails or {}).get("server", None),
                    account=(ds.connectionDetails or {}).get("account", None),
                    database=(ds.connectionDetails or {}).get("database", None),
                    domain=(ds.connectionDetails or {}).get("domain", None),
                )
                for ds in self._client.get_dataset_datasources(workspace.id, wds.id)
            ]

            virtual_view = VirtualView(
                logical_id=VirtualViewLogicalID(
                    name=wds.id, type=VirtualViewType.POWER_BI_DATASET
                ),
                structure=AssetStructure(
                    directories=get_workspace_hierarchy(workspace), name=wds.name
                ),
                power_bi_dataset=VirtualViewPowerBIDataset(
                    tables=get_pbi_dataset_tables(wds),
                    name=wds.name,
                    url=ds.webUrl,
                    description=wds.description,
                    workspace_id=workspace.id,
                    parameters=parameters or None,
                    data_sources=data_sources or None,
                    last_refreshed=last_refreshed,
                    refresh_schedule=refresh_schedule,
                    created_date=safe_parse_ISO8601(wds.createdDate),
                    configured_by=wds.configuredBy,
                    sensitivity_label=await self._graph_client.get_labels(
                        wds.sensitivityLabel.labelId if wds.sensitivityLabel else None
                    ),
                ),
                source_info=SourceInfo(main_url=ds.webUrl),
            )

            self._extract_pipeline_info(wds, virtual_view)

            self._virtual_views[wds.id] = virtual_view

    async def map_wi_reports_to_dashboard(
        self,
        workspace: WorkspaceInfo,
        report_map: Dict[str, PowerBIReport],
        app_map: Dict[str, PowerBIApp],
    ) -> None:
        for wi_report in workspace.reports:
            if wi_report.datasetId is None:
                logger.warning(f"Skipping report without datasetId: {wi_report.id}")
                continue

            virtual_view = self._virtual_views.get(wi_report.datasetId)
            if virtual_view is None:
                logger.error(f"Referenced non-existing dataset {wi_report.datasetId}")
                continue

            upstream_id = str(
                EntityId(EntityType.VIRTUAL_VIEW, virtual_view.logical_id)
            )

            report = report_map.get(wi_report.id, None)
            if report is None:
                logger.warning(f"Skipping invalid report {wi_report.id}")
                continue

            pbi_info = make_power_bi_info(
                PowerBIDashboardType.REPORT, workspace, wi_report, app_map
            )

            pbi_info.sensitivity_label = await self._graph_client.get_labels(
                wi_report.sensitivityLabel.labelId
                if wi_report.sensitivityLabel
                else None
            )

            # The "app" version of report doesn't have pages
            charts = None
            if wi_report.appId is None:
                pages = self._client.get_pages(workspace.id, wi_report.id)
                charts = transform_pages_to_charts(pages)

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=wi_report.id,
                    platform=DashboardPlatform.POWER_BI,
                ),
                structure=AssetStructure(
                    directories=get_workspace_hierarchy(workspace),
                    name=wi_report.name,
                ),
                dashboard_info=DashboardInfo(
                    description=wi_report.description,
                    title=wi_report.name,
                    power_bi=pbi_info,
                    charts=charts,
                    dashboard_type=DashboardType.POWER_BI_REPORT,
                ),
                source_info=SourceInfo(
                    main_url=report.webUrl,
                ),
                entity_upstream=EntityUpstream(source_entities=[upstream_id]),
            )
            self._dashboards[wi_report.id] = dashboard

    async def map_wi_dashboards_to_dashboard(
        self,
        workspace: WorkspaceInfo,
        dashboard_map: Dict[str, PowerBIDashboard],
        app_map: Dict[str, PowerBIApp],
    ) -> None:
        for wi_dashboard in workspace.dashboards:
            tiles = self._client.get_dashboard_tiles(workspace.id, wi_dashboard.id)
            upstream = []
            for tile in tiles:
                dataset_id = tile.datasetId

                # skip tile not depends on a dataset
                if dataset_id == "":
                    continue

                virtual_view = self._virtual_views.get(dataset_id)
                if virtual_view is None:
                    logger.error(f"Referenced non-existing dataset {dataset_id}")
                    continue

                upstream.append(
                    str(EntityId(EntityType.VIRTUAL_VIEW, virtual_view.logical_id))
                )

            pbi_dashboard = dashboard_map.get(wi_dashboard.id, None)
            if pbi_dashboard is None:
                logger.warning(f"Skipping invalid dashboard {wi_dashboard.id}")
                continue

            pbi_info = make_power_bi_info(
                PowerBIDashboardType.DASHBOARD, workspace, wi_dashboard, app_map
            )

            pbi_info.sensitivity_label = await self._graph_client.get_labels(
                wi_dashboard.sensitivityLabel.labelId
                if wi_dashboard.sensitivityLabel
                else None
            )

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=wi_dashboard.id,
                    platform=DashboardPlatform.POWER_BI,
                ),
                structure=AssetStructure(
                    directories=get_workspace_hierarchy(workspace),
                    name=wi_dashboard.displayName,
                ),
                dashboard_info=DashboardInfo(
                    title=wi_dashboard.displayName,
                    charts=transform_tiles_to_charts(tiles),
                    power_bi=pbi_info,
                    dashboard_type=DashboardType.POWER_BI_DASHBOARD,
                ),
                source_info=SourceInfo(
                    main_url=pbi_dashboard.webUrl,
                ),
                entity_upstream=EntityUpstream(source_entities=unique_list(upstream)),
            )
            self._dashboards[wi_dashboard.id] = dashboard

    def dedupe_app_version_dashboards(self):
        for dashboard_id in list(self._dashboards.keys()):
            dashboard = self._dashboards.get(dashboard_id)
            if dashboard.dashboard_info.power_bi.app is None:
                # Non-app dashboard
                continue

            app_url = dashboard.source_info.main_url
            original_dashboard_id = get_dashboard_id_from_url(
                dashboard.source_info.main_url
            )

            if original_dashboard_id == dashboard_id:
                # Shouldn't remove itself
                continue

            original_dashboard = self._dashboards.get(original_dashboard_id)
            if original_dashboard is None:
                # Cannot not find corresponding non-app dashboard
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

    def extract_subscriptions(self, workspaces: List[WorkspaceInfo]):
        users = set(
            user
            for workspace in workspaces
            for user in workspace.users or []
            # Skipping report without datasetId
            if user.principalType == "User"
        )
        subscriptions: Dict[str, PowerBISubscription] = {}

        for user in users:
            user_id = user.graphId
            subscription_user = PowerBiSubscriptionUser(
                emailAddress=user.emailAddress, displayName=user.displayName
            )
            user_subscriptions = self._client.get_user_subscriptions(user_id)

            for user_subscription in user_subscriptions:
                subscription = subscriptions.setdefault(
                    user_subscription.id, user_subscription
                )
                subscription.users.append(subscription_user)

        for subscription in subscriptions.values():
            dashboard = self._dashboards.get(subscription.artifactId)

            if dashboard is None:
                logger.warning(
                    f"Can't found related artifact for subscription: {subscription.id}"
                )
                continue

            power_bi_info = dashboard.dashboard_info.power_bi
            if power_bi_info.subscriptions is None:
                power_bi_info.subscriptions = []

            def safe_parse_date(datetime_str: Optional[str]) -> Optional[datetime]:
                # Example date time 9/12/2023 12:00:00 AM
                datetime_format = "%m/%d/%Y %I:%M:%S %p"
                if not datetime_str:
                    return None
                try:
                    return datetime.strptime(datetime_str, datetime_format).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    logger.warning(f"Unable to parse time: {datetime_str}")
                    return None

            power_bi_info.subscriptions.append(
                Subscription(
                    artifact_display_name=subscription.artifactDisplayName,
                    end_date=safe_parse_date(subscription.endDate),
                    start_date=safe_parse_date(subscription.startDate),
                    sub_artifact_display_name=subscription.subArtifactDisplayName,
                    frequency=subscription.frequency,
                    title=subscription.title,
                    id=subscription.id,
                    users=[
                        SubscriptionUser(
                            email_address=user.emailAddress,
                            display_name=user.displayName,
                        )
                        for user in subscription.users
                    ],
                )
            )

    def extract_activities(self) -> List[UserActivity]:
        res: List[UserActivity] = []

        activities = self._client.get_activities()
        for activity in activities:
            if activity.ArtifactId is None:
                logger.warning("SKIP activity without dashboard id")
                continue

            if activity.UserId is None or not is_email(activity.UserId):
                logger.warning(
                    "SKIP activity without userId or userId isn't in a email format"
                )
                continue

            actor_email = activity.UserId

            entity_id = str(
                to_dashboard_entity_id_from_logical_id(
                    logical_id=DashboardLogicalID(
                        dashboard_id=activity.ArtifactId,
                        platform=DashboardPlatform.POWER_BI,
                    )
                )
            )

            user_id = str(to_person_entity_id(actor_email))

            res.append(
                UserActivity(
                    id=activity.Id,
                    activity_type=UserActivityType.VIEW,
                    source=UserActivitySource.POWER_BI,
                    entity_id=entity_id,
                    duration_in_seconds=0.0,
                    actor=user_id,
                    measure=1.0,
                    timestamp=activity.CreationTime,
                    actor_info=UserActivityActorInfo(email=actor_email),
                )
            )

        return res
