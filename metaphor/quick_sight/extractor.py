from typing import Collection, Dict, List, Optional

from func_timeout import FunctionTimedOut, func_set_timeout

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import to_entity_id_from_virtual_view_logical_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import AssetStructure, Chart, ChartType
from metaphor.models.metadata_change_event import Dashboard as MetaphorDashboard
from metaphor.models.metadata_change_event import DashboardInfo as MetaphorDashboardInfo
from metaphor.models.metadata_change_event import (
    DashboardLogicalID as MetaphorDashboardLogicalId,
)
from metaphor.models.metadata_change_event import (
    DashboardPlatform as MetaphorDashboardPlatform,
)
from metaphor.models.metadata_change_event import (
    EntityUpstream,
    SourceInfo,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from metaphor.quick_sight.client import Client
from metaphor.quick_sight.config import QuickSightRunConfig
from metaphor.quick_sight.data_source_utils import get_id_from_arn
from metaphor.quick_sight.folder import (
    DASHBOARD_DIRECTORIES,
    DATA_SET_DIRECTORIES,
    create_top_level_folders,
)
from metaphor.quick_sight.lineage import LineageProcessor
from metaphor.quick_sight.models import Dashboard, DataSet, ResourceType

logger = get_logger()


class QuickSightExtractor(BaseExtractor):
    """QuickSight metadata extractor"""

    _description = "Quick Sight metadata crawler"
    _platform = Platform.QUICK_SIGHT

    @staticmethod
    def from_config_file(config_file: str) -> "QuickSightExtractor":
        return QuickSightExtractor(QuickSightRunConfig.from_yaml_file(config_file))

    def __init__(self, config: QuickSightRunConfig) -> None:
        super().__init__(config)
        self._aws_config = config.aws
        self._aws_account_id = config.aws_account_id

        # Arn -> Resource
        self._resources: Dict[str, ResourceType] = {}

        # DataSetId -> VirtualView
        self._virtual_views: Dict[str, VirtualView] = {}

        # DashboardId -> Dashboard
        self._dashboards: Dict[str, MetaphorDashboard] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from QuickSight")

        client = Client(self._aws_config, self._aws_account_id, self._resources)
        client.get_resources()

        self._extract_virtual_views()
        self._extract_dashboards()

        return self._make_entities_list()

    @func_set_timeout(20)
    def _extract_virtual_view(self, data_set: DataSet) -> None:
        view = self._init_virtual_view(data_set)
        output_logical_table_id = LineageProcessor(
            self._resources, self._virtual_views, data_set
        ).run()

        # The last logical_table is the output table for the dataset
        output = self._virtual_views.get(output_logical_table_id)
        if output:
            view.schema = output.schema
            view.entity_upstream = output.entity_upstream
            self._virtual_views.pop(output_logical_table_id)

    def _extract_virtual_views(self):
        count = 0
        for data_set in self._resources.values():
            if not isinstance(data_set, DataSet):
                continue

            try:
                self._extract_virtual_view(data_set)
            except FunctionTimedOut:
                logger.warning(
                    f"Timeout when extracting virtual view from DataSet {data_set.Arn}"
                )
                continue

            count += 1
            if count % 100 == 0:
                logger.info(f"Parsed {count} virtual views")

        logger.info(f"Parsed {count} virtual views")

    def _extract_dashboards(self) -> None:
        count = 0
        for dashboard in self._resources.values():
            if not isinstance(dashboard, Dashboard) or dashboard.Version is None:
                continue

            metaphor_dashboard = self._init_dashboard(dashboard)
            metaphor_dashboard.entity_upstream = self._get_dashboard_upstream(
                dataset_arns=dashboard.Version.DataSetArns or []
            )

            count += 1
            if count % 100 == 0:
                logger.info(f"Parsed {count} dashboards")

        logger.info(f"Parsed {count} dashboards")

    def _make_entities_list(self) -> Collection[ENTITY_TYPES]:
        entities: List[ENTITY_TYPES] = []
        entities.extend(self._virtual_views.values())
        entities.extend(self._dashboards.values())
        entities.extend(create_top_level_folders())
        return entities

    def _init_virtual_view(self, data_set: DataSet) -> VirtualView:
        data_set_id = data_set.DataSetId
        assert data_set_id

        view = VirtualView(
            logical_id=VirtualViewLogicalID(
                name=data_set_id,
                type=VirtualViewType.QUICK_SIGHT,
            ),
            structure=AssetStructure(
                name=data_set.Name, directories=DATA_SET_DIRECTORIES
            ),
            source_info=SourceInfo(
                created_at_source=data_set.CreatedTime,
                last_updated=data_set.LastUpdatedTime,
            ),
        )

        self._virtual_views[data_set_id] = view
        return view

    def _init_dashboard(self, dashboard: Dashboard) -> MetaphorDashboard:
        dashboard_id = dashboard.DashboardId
        assert dashboard_id
        assert dashboard.Version

        metaphor_dashboard = MetaphorDashboard(
            logical_id=MetaphorDashboardLogicalId(
                dashboard_id=dashboard_id,
                platform=MetaphorDashboardPlatform.QUICK_SIGHT,
            ),
            source_info=SourceInfo(
                created_at_source=dashboard.CreatedTime,
                last_updated=dashboard.LastUpdatedTime,
            ),
            structure=AssetStructure(
                name=dashboard.Name,
                directories=DASHBOARD_DIRECTORIES,
            ),
        )

        sheets = dashboard.Version.Sheets or []

        metaphor_dashboard.dashboard_info = MetaphorDashboardInfo(
            description=dashboard.Version.Description,
            title=dashboard.Name,
            charts=[
                Chart(
                    chart_type=ChartType.OTHER,
                    title=sheet.Name,
                    url=None,
                )
                for sheet in sheets
            ],
        )

        self._dashboards[dashboard_id] = metaphor_dashboard
        return metaphor_dashboard

    def _get_dashboard_upstream(
        self, dataset_arns: List[str]
    ) -> Optional[EntityUpstream]:
        source_entities: List[str] = []

        for arn in dataset_arns:
            dataset_id = get_id_from_arn(arn)
            virtual_view = self._virtual_views.get(dataset_id)
            if not virtual_view:
                logger.warning(f"Virtual view not found for dataset {dataset_id}")
                continue

            source_entities.append(
                str(to_entity_id_from_virtual_view_logical_id(virtual_view.logical_id))
            )

        if not source_entities:
            return None

        return EntityUpstream(source_entities=(unique_list(source_entities)))
