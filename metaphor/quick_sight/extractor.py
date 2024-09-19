import enum
from typing import Collection, Dict, List

import boto3
from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import to_entity_id_from_virtual_view_logical_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
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
    Dataset,
    EntityUpstream,
    SourceInfo,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from metaphor.quick_sight.cll import process_dataset_column_lineage
from metaphor.quick_sight.config import AwsCredentials, QuickSightRunConfig
from metaphor.quick_sight.models import (
    Analysis,
    Dashboard,
    DataSet,
    DataSource,
    Folder,
    ResourceType,
    User,
)

logger = get_logger()


def create_quick_sight_client(aws: AwsCredentials) -> boto3.client:
    return aws.get_session().client("quicksight")


class Endpoint(enum.Enum):
    list_dashboards = "list_dashboards"
    list_data_sets = "list_data_sets"
    list_data_sources = "list_data_sources"
    list_analyses = "list_analyses"
    list_folders = "list_folders"
    list_users = "list_users"


@dataclass
class EndpointDictKeys:
    list_key: str
    item_key: str


ENDPOINT_SETTING = {
    Endpoint.list_data_sets: EndpointDictKeys("DataSetSummaries", "DataSetId"),
    Endpoint.list_dashboards: EndpointDictKeys("DashboardSummaryList", "DashboardId"),
    Endpoint.list_analyses: EndpointDictKeys("AnalysisSummaryList", "AnalysisId"),
    Endpoint.list_data_sources: EndpointDictKeys("DataSources", "DataSourceId"),
    Endpoint.list_folders: EndpointDictKeys("FolderSummaryList", "FolderId"),
}


class QuickSightExtractor(BaseExtractor):
    """QuickSight metadata extractor"""

    _description = "Quick Sight metadata crawler"
    _platform = Platform.QUICK_SIGHT

    @staticmethod
    def from_config_file(config_file: str) -> "QuickSightExtractor":
        return QuickSightExtractor(QuickSightRunConfig.from_yaml_file(config_file))

    def __init__(self, config: QuickSightRunConfig) -> None:
        super().__init__(config)
        self._datasets: Dict[str, Dataset] = {}
        self._aws_config = config.aws
        self._aws_account_id = config.aws_account_id

        # Arn -> Resource
        self._resources: Dict[str, ResourceType] = {}

        # Arn -> VirtualView
        self._virtual_views: Dict[str, VirtualView] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from QuickSight")

        self._client = create_quick_sight_client(self._aws_config)

        self._get_resources()

        self._extract_virtual_view()

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._virtual_views.values())
        entities.extend(self._extract_dashboard())

        return entities

    def _get_resources(self):
        self._get_dataset_detail()
        self._get_dashboard_detail()
        self._get_analysis_detail()
        self._get_data_source_detail()
        self._get_folder_detail()
        self._get_user_detail()

    def _extract_virtual_view(self):
        for data_set in self._resources.values():
            if not isinstance(data_set, DataSet):
                continue

            view = VirtualView(
                logical_id=VirtualViewLogicalID(
                    name=data_set.Arn,
                    type=VirtualViewType.QUICK_SIGHT,
                ),
                structure=AssetStructure(name=data_set.Name),
                source_info=SourceInfo(
                    created_at_source=data_set.CreatedTime,
                    last_updated=data_set.LastUpdatedTime,
                ),
            )

            process_dataset_column_lineage(self._resources, data_set, view)

            self._virtual_views[data_set.Arn] = view

    def _extract_dashboard(self) -> List[MetaphorDashboard]:
        dashboards: List[MetaphorDashboard] = []
        for dashboard in self._resources.values():
            if (
                not isinstance(dashboard, Dashboard)
                or dashboard.Arn is None
                or dashboard.Version is None
            ):
                continue

            metaphor_dashboard = MetaphorDashboard(
                logical_id=MetaphorDashboardLogicalId(
                    dashboard_id=dashboard.Arn,
                    platform=MetaphorDashboardPlatform.QUICK_SIGHT,
                ),
                source_info=SourceInfo(
                    created_at_source=dashboard.CreatedTime,
                    last_updated=dashboard.LastUpdatedTime,
                ),
                structure=AssetStructure(
                    name=dashboard.Name,
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
                        url=None,  # TODO: embed URL
                    )
                    for sheet in sheets
                ],
            )

            source_entities: List[str] = []

            for arn in dashboard.Version.DataSetArns or []:
                virtual_view = self._virtual_views.get(arn)
                if not virtual_view:
                    continue
                source_entities.append(
                    str(
                        to_entity_id_from_virtual_view_logical_id(
                            virtual_view.logical_id
                        )
                    )
                )

            metaphor_dashboard.entity_upstream = EntityUpstream(
                source_entities=(
                    unique_list(source_entities) if source_entities else None
                )
            )

            dashboards.append(metaphor_dashboard)

        return dashboards

    def _get_resource_ids(self, endpoint: Endpoint) -> List[str]:
        paginator = self._client.get_paginator(endpoint.value)
        paginator_response = paginator.paginate(AwsAccountId=self._aws_account_id)

        ids = []
        settings = ENDPOINT_SETTING[endpoint]
        for page in paginator_response:
            for item in page[settings.list_key]:
                ids.append(item[settings.item_key])
        return ids

    def _get_dataset_detail(self) -> None:
        results = []
        for dataset_id in self._get_resource_ids(Endpoint.list_data_sets):
            result = self._client.describe_data_set(
                AwsAccountId=self._aws_account_id, DataSetId=dataset_id
            )

            results.append(result)

            dataset = DataSet(**(result["DataSet"]))

            if dataset.Arn is None:
                continue

            self._resources[dataset.Arn] = dataset

        json_dump_to_debug_file(results, "datasets.json")

    def _get_dashboard_detail(self):
        results = []
        for dashboard_id in self._get_resource_ids(Endpoint.list_dashboards):
            result = self._client.describe_dashboard(
                AwsAccountId=self._aws_account_id, DashboardId=dashboard_id
            )
            results.append(result)
            dashboard = Dashboard(**(result["Dashboard"]))

            if dashboard.Arn is None:
                continue

            self._resources[dashboard.Arn] = dashboard

        json_dump_to_debug_file(results, "dashboards.json")

    def _get_analysis_detail(self):
        results = []
        for analysis_id in self._get_resource_ids(Endpoint.list_analyses):
            result = self._client.describe_analysis(
                AwsAccountId=self._aws_account_id, AnalysisId=analysis_id
            )
            results.append(result)
            arn = result["Analysis"]["Arn"]
            definition = self._client.describe_analysis_definition(
                AwsAccountId=self._aws_account_id, AnalysisId=analysis_id
            )

            analysis = Analysis(**definition, Arn=arn)

            if analysis.Arn is None:
                continue

            self._resources[analysis.Arn] = analysis

        json_dump_to_debug_file(results, "analyses.json")

    def _get_data_source_detail(self):
        results = []
        for data_source_id in self._get_resource_ids(Endpoint.list_data_sources):
            result = self._client.describe_data_source(
                AwsAccountId=self._aws_account_id, DataSourceId=data_source_id
            )
            results.append(result)

            data_source = DataSource(**(result["DataSource"]))

            if data_source.Arn is None:
                continue

            self._resources[data_source.Arn] = data_source

        json_dump_to_debug_file(results, "data_sources.json")

    def _get_folder_detail(self):
        results = []
        for folder_id in self._get_resource_ids(Endpoint.list_folders):
            result = self._client.describe_folder(
                AwsAccountId=self._aws_account_id, FolderId=folder_id
            )
            results.append(result)

            folder = Folder(**result["Folder"])

            if folder.Arn is None:
                continue

            self._resources[folder.Arn] = folder

        json_dump_to_debug_file(results, "folders.json")

    def _get_user_detail(self):
        paginator = self._client.get_paginator(Endpoint.list_users.value)
        paginator_response = paginator.paginate(
            AwsAccountId=self._aws_account_id, Namespace="default"
        )

        users: List[User] = []
        results = []
        for page in paginator_response:
            for item in page["UserList"]:
                results.append(item)
                user = User(**item)

                if user.Arn is None:
                    continue

                users.append(user)
                self._resources[user.Arn] = user

        json_dump_to_debug_file(results, "users.json")
        return users
