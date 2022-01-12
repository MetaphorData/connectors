import base64
import re
from typing import Dict, List, Optional

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
    DashboardPlatform,
    MetadataChangeEvent,
)

from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.tableau.config import TableauRunConfig

logger = get_logger(__name__)


class TableauExtractor(BaseExtractor):
    """Tableau metadata extractor"""

    def __init__(self):
        self._base_url: Optional[str] = None
        self._views: Dict[str, ViewItem] = {}
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

            views: List[ViewItem] = [item for item in Pager(server.views, usage=True)]
            logger.info(
                f"\nThere are {len(views)} views on site: {[view.name for view in views]}"
            )
            for item in views:
                server.views.populate_preview_image(item)
                self._views[item.id] = item

            workbooks: List[WorkbookItem] = [item for item in Pager(server.workbooks)]
            logger.info(
                f"\nThere are {len(workbooks)} work books on site: {[workbook.name for workbook in workbooks]}"
            )
            for item in workbooks:
                server.workbooks.populate_connections(item)
                server.workbooks.populate_views(item, usage=True)

                if not self._base_url:
                    self._get_base_url(item.webpage_url)

                self._parse_dashboard(item)

        return [EventUtil.build_dashboard_event(d) for d in self._dashboards.values()]

    def _parse_dashboard(self, workbook: WorkbookItem) -> None:
        views: List[ViewItem] = workbook.views
        charts = [self._parse_chart(view) for view in views]
        total_views = sum([view.total_views for view in views])

        dashboard_info = DashboardInfo(
            title=f"{workbook.project_name}.{workbook.name}",
            description=workbook.description,
            url=workbook.webpage_url,
            charts=charts,
            view_count=float(total_views),
        )

        dashboard = Dashboard(
            logical_id=DashboardLogicalID(
                dashboard_id=workbook.id, platform=DashboardPlatform.TABLEAU
            ),
            dashboard_info=dashboard_info,
        )

        self._dashboards[workbook.id] = dashboard

    def _parse_chart(self, view: ViewItem) -> Chart:
        original_view = self._views.get(view.id)
        # encode preview image raw bytes into data URL
        preview_data_url = (
            TableauExtractor._build_preview_data_url(original_view.preview_image)
            if original_view and original_view.preview_image
            else None
        )

        view_url = self._build_view_url(view.content_url)

        return Chart(title=view.name, url=view_url, preview=preview_data_url)

    _workbook_url_regex = r"(.+\/site\/\w+)\/workbooks\/(\w+)(\/.*)*"

    def _get_base_url(self, workbook_url: str) -> None:
        match = re.search(self._workbook_url_regex, workbook_url)
        if match:
            self._base_url = match.group(1)

    def _build_view_url(self, content_url: str) -> Optional[str]:
        """
        Builds view URL from the API content_url field.
        content_url is in the form of <workbook>/sheets/<view>, e.g. 'Superstore/sheets/WhatIfForecast'
        """
        workbook, _, view = content_url.split("/")

        return f"{self._base_url}/views/{workbook}/{view}" if self._base_url else None

    @staticmethod
    def _build_preview_data_url(preview: bytes) -> str:
        return f"data:image/png;base64,{base64.b64encode(preview).decode('ascii')}"
