import os
from typing import Collection, Dict, Iterable, List, Sequence, Set, Tuple

from metaphor.common.git import clone_repo
from metaphor.models.crawler_run_metadata import Platform

try:
    import looker_sdk
    from looker_sdk.sdk.api40.models import DashboardElement
except ImportError:
    print("Please install metaphor[looker] extra\n")
    raise

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import to_virtual_view_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.looker.config import LookerConnectionConfig, LookerRunConfig
from metaphor.looker.folder import FolderMap, FolderMetadata, build_directories
from metaphor.looker.lookml_parser import ModelMap, fullname, parse_project
from metaphor.models.metadata_change_event import (
    AssetPlatform,
    AssetStructure,
    Chart,
    ChartType,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    EntityUpstream,
    Hierarchy,
    SourceInfo,
    SystemContact,
    SystemContacts,
    SystemDescription,
    VirtualViewType,
)

logger = get_logger()


class LookerExtractor(BaseExtractor):
    """Looker metadata extractor"""

    _description = "Looker metadata crawler"
    _platform = Platform.LOOKER

    vis_type_map = {
        "looker_area": ChartType.AREA,
        "looker_bar": ChartType.BAR,
        "looker_boxplot": ChartType.BOX_PLOT,
        "looker_column": ChartType.COLUMN,
        "looker_donut_multiples": ChartType.DONUT,
        "looker_line": ChartType.LINE,
        "looker_map": ChartType.MAP,
        "looker_geo_coordinates": ChartType.MAP,
        "looker_geo_choropleth": ChartType.MAP,
        "looker_pie": ChartType.PIE,
        "looker_scatter": ChartType.SCATTER,
        "table": ChartType.TABLE,
        "looker_grid": ChartType.TABLE,
        "looker_single_record": ChartType.TABLE,
        "single_value": ChartType.TEXT,
        "text": ChartType.TEXT,
    }

    @staticmethod
    def from_config_file(config_file: str) -> "LookerExtractor":
        return LookerExtractor(LookerRunConfig.from_yaml_file(config_file))

    def __init__(self, config: LookerRunConfig) -> None:
        super().__init__(config)
        self._base_url = config.base_url
        self._alternative_base_url = config.alternative_base_url
        self._connections = config.connections
        self._lookml_dir = config.lookml_dir
        self._lookml_git_repo = config.lookml_git_repo
        self._ignored_model_files = config.ignored_model_files or []
        self._project_source_url = config.project_source_url
        self._include_personal_folders = config.include_personal_folders
        self._explore_view_folder_name = config.explore_view_folder_name
        self._folders: Dict[str, Hierarchy] = {}

        # Load config using environment variables instead from looker.ini file
        # See https://github.com/looker-open-source/sdk-codegen#environment-variable-configuration
        os.environ["LOOKERSDK_BASE_URL"] = self._base_url
        os.environ["LOOKERSDK_CLIENT_ID"] = config.client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = config.client_secret
        os.environ["LOOKERSDK_VERIFY_SSL"] = str(config.verify_ssl)
        os.environ["LOOKERSDK_TIMEOUT"] = str(config.timeout)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Looker")

        self._sdk = looker_sdk.init40()

        # Lower case all connection names for case-insensitive lookup
        connections: Dict[str, LookerConnectionConfig] = {
            k.lower(): v for (k, v) in self._connections.items()
        }

        lookml_dir = self._lookml_dir
        if not lookml_dir:
            assert self._lookml_git_repo, "missing git repo config"
            lookml_dir = clone_repo(self._lookml_git_repo)
        logger.info(f"Parsing LookML project at {lookml_dir}")

        model_map, virtual_views = parse_project(
            lookml_dir,
            connections,
            self._explore_view_folder_name,
            self._project_source_url,
            self._ignored_model_files,
        )

        folder_map = self._fetch_folders()
        user_map = self._fetch_users()

        dashboards = self._fetch_dashboards(model_map, folder_map, user_map)

        entities: List[ENTITY_TYPES] = []
        entities.extend(dashboards)
        entities.extend(virtual_views)
        entities.extend(self._folders.values())
        return entities

    def _fetch_folders(self) -> FolderMap:
        folder_map: FolderMap = {}
        folders = self._sdk.all_folders()
        json_dump_to_debug_file(folders, "all_folders.json")

        for folder in folders:
            if folder.id is None:
                continue
            folder_map[folder.id] = FolderMetadata(
                id=folder.id, name=folder.name, parent_id=folder.parent_id
            )

        return folder_map

    def _fetch_users(self) -> Dict[str, str]:
        user_map: Dict[str, str] = {}
        users = self._sdk.all_users()
        json_dump_to_debug_file(users, "all_users.json")

        for user in users:
            if user.email is None or user.id is None:
                continue
            user_map[user.id] = user.email

        return user_map

    def _fetch_dashboards(
        self, model_map: ModelMap, folder_map: FolderMap, user_map: Dict[str, str]
    ) -> List[Dashboard]:
        dashboards: List[Dashboard] = []

        all_dashboards = self._sdk.all_dashboards()
        json_dump_to_debug_file(all_dashboards, "all_dashboards.json")

        for basic_dashboard in all_dashboards:
            assert basic_dashboard.id is not None

            try:
                dashboard = self._sdk.dashboard(dashboard_id=basic_dashboard.id)
                json_dump_to_debug_file(
                    dashboard, f"{basic_dashboard.id}_dashboard.json"
                )
            except Exception as error:
                logger.error(f"Failed to fetch dashboard {basic_dashboard.id}: {error}")
                continue

            # Skip personal folders
            if (
                not self._include_personal_folders
                and dashboard.folder
                and (
                    dashboard.folder.is_personal
                    or dashboard.folder.is_personal_descendant
                )
            ):
                logger.info(f"Skipping personal dashboard {dashboard.id}")
                continue

            logger.info(f"Processing dashboard {basic_dashboard.id}")

            dashboard_info = DashboardInfo(
                title=dashboard.title,
                description=dashboard.description,
                charts=[],
            )

            # Prefer user-provided base url
            base_url = self._alternative_base_url or self._base_url

            # preferred_viewer only have value if the dashboard support new version (dashboard-next)
            prefix = dashboard.preferred_viewer or "dashboards"

            source_info = SourceInfo(
                main_url=f"{base_url}/{prefix}/{dashboard.id}",
            )

            # All numeric fields must be converted to "float" to meet quicktype's expectation
            if dashboard.view_count is not None:
                dashboard_info.view_count = float(dashboard.view_count)

            entity_upstream = None
            if dashboard.dashboard_elements is not None:
                (dashboard_info.charts, entity_upstream) = self._extract_charts(
                    dashboard.dashboard_elements, model_map
                )

            assert dashboard.id is not None

            owner_email = user_map.get(dashboard.user_id or "")
            system_contacts = (
                SystemContacts(
                    contacts=[
                        SystemContact(
                            email=owner_email,
                            system_contact_source=AssetPlatform.LOOKER,
                        )
                    ]
                )
                if owner_email
                else None
            )

            dashboards.append(
                Dashboard(
                    logical_id=DashboardLogicalID(
                        dashboard.id, DashboardPlatform.LOOKER
                    ),
                    dashboard_info=dashboard_info,
                    source_info=source_info,
                    entity_upstream=entity_upstream,
                    structure=(
                        AssetStructure(
                            directories=build_directories(
                                dashboard.folder.id,
                                folder_map,
                                self._folders,
                            ),
                            name=dashboard.title,
                        )
                        if dashboard.folder and dashboard.folder.id
                        else None
                    ),
                    system_contacts=system_contacts,
                    system_description=(
                        SystemDescription(
                            description=dashboard.description,
                            platform=AssetPlatform.LOOKER,
                        )
                        if dashboard.description
                        else None
                    ),
                )
            )

        return dashboards

    def _extract_charts(
        self,
        dashboard_elements: Sequence[DashboardElement],
        model_map: ModelMap,
    ) -> Tuple[List[Chart], EntityUpstream]:
        charts = []
        explore_ids: Set[str] = set()

        for e in filter(lambda e: e.type == "vis", dashboard_elements):
            if e.result_maker is None:
                logger.warning(f"Unable to find result_maker in element {e.title}")
                continue

            chart_type = None
            if e.result_maker.vis_config is not None:
                chart_type = self.vis_type_map.get(
                    e.result_maker.vis_config.get("type", ""), ChartType.OTHER
                )

            charts.append(
                Chart(
                    # Use "id" if "title" is None or empty string
                    title=e.title if e.title else e.id,
                    description=e.note_text,
                    chart_type=chart_type,
                )
            )

            if not isinstance(e.result_maker.filterables, Iterable):
                logger.warning(f"Unable to iterate filterables in element {e.title}")
                continue

            for f in e.result_maker.filterables:
                if f.model is None or f.view is None:
                    logger.warning(f"Missing model or view in element {e.title}")
                    continue

                model = model_map.get(f.model)
                if model is None:
                    logger.error(f"Chart {e.title} references invalid model {f.model}")
                    continue

                explore = model.explores.get(f.view)
                if explore is None:
                    logger.error(f"Chart {e.title} references invalid explore {f.view}")
                    continue

                explore_ids.add(
                    str(
                        to_virtual_view_entity_id(
                            fullname(f.model, explore.name),
                            VirtualViewType.LOOKER_EXPLORE,
                        )
                    )
                )

        return (
            charts,
            EntityUpstream(
                source_entities=list(explore_ids),
            ),
        )
