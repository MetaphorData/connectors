import logging
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

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
    DataPlatform,
    MetadataChangeEvent,
)
from serde import deserialize

from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor, RunConfig
from metaphor.looker.lookml_parser import Connection, Model, parse_models

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class LookerRunConfig(RunConfig):
    base_url: str
    client_id: str
    client_secret: str

    lookml_dir: str

    verify_ssl: bool = True
    timeout: int = 120


class LookerExtractor(BaseExtractor):
    """Looker metadata extractor"""

    @staticmethod
    def config_class():
        return LookerRunConfig

    # From https://docs.looker.com/setup-and-management/database-config
    dialect_platform_map = {
        "snowflake": DataPlatform.SNOWFLAKE,
        "mysql": DataPlatform.MYSQL,
        "mysql_8": DataPlatform.MYSQL,
        "postgres": DataPlatform.POSTGRESQL,
        "redshift": DataPlatform.REDSHIFT,
    }

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

        connections = self._fetch_connections(sdk)

        model_map = parse_models(config.lookml_dir, connections)

        dashboards = self._fetch_dashboards(config, sdk, model_map)
        return [EventUtil.build_dashboard_event(d) for d in dashboards]

    def _fetch_connections(self, sdk: Looker31SDK) -> Dict[str, Connection]:
        connection_map = {}
        for connection in sdk.all_connections():
            assert connection.name is not None
            assert connection.database is not None

            name = connection.name
            dialect = connection.dialect_name

            if dialect not in self.dialect_platform_map:
                logger.warn(
                    f"Ignore connection {name} with unsupported dialect {dialect}"
                )
                continue
            platform = self.dialect_platform_map[dialect]

            connection_map[name] = Connection(
                name=connection.name,
                platform=platform,
                database=connection.database,
                account=self.parse_account(connection.host, platform),
                default_schema=connection.schema,
            )

        return connection_map

    @staticmethod
    def parse_account(host: Optional[str], platform: DataPlatform) -> Optional[str]:
        if platform == DataPlatform.SNOWFLAKE and host:
            # Snowflake host <account_name>.snowflakecomputing.com
            # see https://docs.looker.com/setup-and-management/database-config/snowflake
            return host.split(".")[0]
        return None

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

        dataset_ids: Set[str] = set()
        charts = []
        for e in filter(lambda e: e.type == "vis", dashboard_elements):

            if e.result_maker is None:
                logger.warn(f"Unable to find result_maker is element {e.title}")
                continue

            chart_type = None
            if e.result_maker.vis_config is not None:
                chart_type = self.vis_type_map.get(
                    e.result_maker.vis_config.get("type", ""), ChartType.OTHER
                )

            charts.append(Chart(title=e.title, chart_type=chart_type))

            if not isinstance(e.result_maker.filterables, Iterable):
                logger.warn(f"Unable to iterate filterables in element {e.title}")
                continue

            for f in e.result_maker.filterables:
                if f.model is None or f.view is None:
                    logger.warn(f"Missing model or view in element {e.title}")
                    continue

                model = model_map.get(f.model, None)
                if model is None:
                    raise ValueError(
                        f"Chart {e.title} references invalid model {f.model}"
                    )

                explore = model.explores.get(f.view, None)
                if explore is None:
                    raise ValueError(
                        f"Chart {e.title} references invalid explore {f.view}"
                    )

                for id in explore.upstream_datasets:
                    dataset_ids.add(str(id))

        return (
            charts,
            DashboardUpstream(
                source_datasets=list(dataset_ids),
            ),
        )
