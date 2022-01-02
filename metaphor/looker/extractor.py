import os
from typing import Dict, Iterable, List, Sequence, Set, Tuple

try:
    import looker_sdk
    from looker_sdk.sdk.api31.methods import Looker31SDK
    from looker_sdk.sdk.api31.models import DashboardElement
except ImportError:
    print("Please install metaphor[looker] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    Chart,
    ChartType,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    MetadataChangeEvent,
    VirtualViewType,
)

from metaphor.common.entity_id import to_virtual_view_entity_id
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.looker.config import LookerConnectionConfig, LookerRunConfig
from metaphor.looker.lookml_parser import Model, fullname, parse_project

logger = get_logger(__name__)


class LookerExtractor(BaseExtractor):
    """Looker metadata extractor"""

    @staticmethod
    def config_class():
        return LookerRunConfig

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

    def initSdk(self, config: LookerRunConfig) -> Looker31SDK:
        # Load config using environment variables instead from looker.ini file
        # See https://github.com/looker-open-source/sdk-codegen#environment-variable-configuration
        os.environ["LOOKERSDK_BASE_URL"] = config.base_url
        os.environ["LOOKERSDK_CLIENT_ID"] = config.client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = config.client_secret
        os.environ["LOOKERSDK_VERIFY_SSL"] = str(config.verify_ssl)
        os.environ["LOOKERSDK_TIMEOUT"] = str(config.timeout)
        return looker_sdk.init31()

    async def extract(self, config: LookerRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, LookerExtractor.config_class())

        logger.info("Fetching metadata from Looker")

        sdk = self.initSdk(config)

        # Lower case all connection names for case-insensitive lookup
        connections: Dict[str, LookerConnectionConfig] = {
            k.lower(): v for (k, v) in config.connections.items()
        }

        model_map, virtual_views = parse_project(
            config.lookml_dir, connections, config.project_source_url
        )

        dashboards = self._fetch_dashboards(config, sdk, model_map)

        dashboard_events = [EventUtil.build_dashboard_event(d) for d in dashboards]
        virtual_view_events = [
            EventUtil.build_virtual_view_event(d) for d in virtual_views
        ]
        return dashboard_events + virtual_view_events

    def _fetch_dashboards(
        self, config: LookerRunConfig, sdk: Looker31SDK, model_map: Dict[str, Model]
    ) -> List[Dashboard]:
        dashboards: List[Dashboard] = []
        for basic_dashboard in sdk.all_dashboards():
            assert basic_dashboard.id is not None
            dashboard = sdk.dashboard(dashboard_id=basic_dashboard.id)

            dashboard_info = DashboardInfo()
            dashboard_info.title = dashboard.title
            dashboard_info.description = dashboard.description
            dashboard_info.url = (
                f"{config.base_url}/{dashboard.preferred_viewer}/{dashboard.id}"
            )

            # All numeric fields must be converted to "float" to meet quicktype's expectation
            if dashboard.view_count is not None:
                dashboard_info.view_count = float(dashboard.view_count)

            dashboard_info.charts = []
            upstream = None
            if dashboard.dashboard_elements is not None:
                (dashboard_info.charts, upstream) = self._extract_charts(
                    dashboard.dashboard_elements, model_map
                )

            dashboards.append(
                Dashboard(
                    logical_id=DashboardLogicalID(
                        dashboard.id, DashboardPlatform.LOOKER
                    ),
                    dashboard_info=dashboard_info,
                    upstream=upstream,
                )
            )

        return dashboards

    def _extract_charts(
        self,
        dashboard_elements: Sequence[DashboardElement],
        model_map: Dict[str, Model],
    ) -> Tuple[List[Chart], DashboardUpstream]:
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
            DashboardUpstream(
                source_virtual_views=list(explore_ids),
            ),
        )
