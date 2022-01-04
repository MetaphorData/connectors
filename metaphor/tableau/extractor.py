import json
from typing import Dict, List

from metaphor.common.event_util import EventUtil

try:
    from tableauserverclient import (
        DatasourceItem,
        Pager,
        PersonalAccessTokenAuth,
        Server,
        TableauAuth,
        ViewItem,
        WorkbookItem,
    )
except ImportError:
    print("Please install metaphor[tableau] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    MetadataChangeEvent,
)

from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.tableau.config import TableauRunConfig

logger = get_logger(__name__)


class TableauExtractor(BaseExtractor):
    """Tableau metadata extractor"""

    def __init__(self):
        self._dashboards: Dict[str, Dashboard] = {}

    @staticmethod
    def config_class():
        return TableauRunConfig

    async def extract(self, config: TableauRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, TableauExtractor.config_class())

        logger.info("Fetching metadata from Tableau")

        assert (
            config.access_token or config.user_password
        ), "no login credential, please provide access token or user password in config"

        tableau_auth = (
            PersonalAccessTokenAuth(
                config.access_token.token_name,
                config.access_token.token_value,
                config.site_name,
            )
            if config.access_token
            else TableauAuth(
                config.user_password.username,
                config.user_password.password,
                config.site_name,
            )
        )
        server = Server(config.server_url, use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            data_sources: List[DatasourceItem] = [
                item for item in Pager(server.datasources)
            ]
            logger.info(
                f"\nThere are {len(data_sources)} data sources on site: {[datasource.name for datasource in data_sources]}"
            )
            for item in data_sources:
                server.datasources.populate_connections(item)

            workbooks: List[WorkbookItem] = [item for item in Pager(server.workbooks)]
            logger.info(
                f"\nThere are {len(workbooks)} work books on site: {[workbook.name for workbook in workbooks]}"
            )
            for item in workbooks:
                server.workbooks.populate_connections(item)
                server.workbooks.populate_views(item, usage=True)
            logger.info(
                [json.dumps(workbook.__dict__, default=str) for workbook in workbooks]
            )
            for workbook in workbooks:
                self.parse_dashboard(workbook)

        return [EventUtil.build_dashboard_event(d) for d in self._dashboards.values()]

    def parse_dashboard(self, workbook: WorkbookItem) -> None:
        views: List[ViewItem] = workbook.views
        charts = [Chart(title=view.name) for view in views]
        total_views = sum([view.total_views for view in views])

        dashboard_info = DashboardInfo(
            title=f"{workbook.project_name}.{workbook.name}",
            description=workbook.description,
            url=workbook.webpage_url,
            charts=charts,
            view_count=float(total_views),
        )

        dashboard = Dashboard(
            logical_id=DashboardLogicalID(dashboard_id=workbook.id),
            dashboard_info=dashboard_info,
        )

        self._dashboards[workbook.id] = dashboard
