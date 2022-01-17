import base64
import re
import traceback
from typing import Dict, List, Optional, Tuple

from metaphor.common.entity_id import EntityId, to_dataset_entity_id

try:
    from tableauserverclient import (
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
    DashboardUpstream,
    DataPlatform,
    MetadataChangeEvent,
)

from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.tableau.config import TableauRunConfig
from metaphor.tableau.query import connection_type_map, workbooks_graphql_query

logger = get_logger(__name__)


class TableauExtractor(BaseExtractor):
    """Tableau metadata extractor"""

    def __init__(self):
        self._base_url: Optional[str] = None
        self._views: Dict[str, ViewItem] = {}
        self._dashboards: Dict[str, Dashboard] = {}
        self._snowflake_account = None

    @staticmethod
    def config_class():
        return TableauRunConfig

    async def extract(self, config: TableauRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, TableauExtractor.config_class())

        logger.info("Fetching metadata from Tableau")

        self._snowflake_account = config.snowflake_account

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
            # fetch all views, with preview image
            views: List[ViewItem] = [item for item in Pager(server.views, usage=True)]
            logger.info(
                f"There are {len(views)} views on site: {[view.name for view in views]}\n"
            )
            for item in views:
                server.views.populate_preview_image(item)
                self._views[item.id] = item

            # fetch all workbooks
            workbooks: List[WorkbookItem] = [item for item in Pager(server.workbooks)]
            logger.info(
                f"\nThere are {len(workbooks)} work books on site: {[workbook.name for workbook in workbooks]}"
            )
            for item in workbooks:
                server.workbooks.populate_views(item, usage=True)

                try:
                    self._parse_dashboard(item)
                except Exception as error:
                    traceback.print_exc()
                    logger.error(f"failed to parse workbook {item.name}, error {error}")

            # fetch workbook upstreams
            # NOTE!!! the id (uuid) returned by graphql api for a particular entity is different with the one returned
            # by the REST api, can NOT use id to match entities.
            resp = server.metadata.query(workbooks_graphql_query)
            resp_data = resp["data"]
            for item in resp_data["workbooks"]:
                try:
                    self._parse_dashboard_upstream(item)
                except Exception as error:
                    traceback.print_exc()
                    logger.error(
                        f"failed to parse workbook upstream {item['vizportalUrlId']}, error {error}"
                    )

        return [EventUtil.build_dashboard_event(d) for d in self._dashboards.values()]

    def _parse_dashboard(self, workbook: WorkbookItem) -> None:
        base_url, workbook_id = TableauExtractor._parse_workbook_url(
            workbook.webpage_url
        )
        if not self._base_url:
            self._base_url = base_url

        views: List[ViewItem] = workbook.views
        charts = [self._parse_chart(self._views[view.id]) for view in views]
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
                dashboard_id=workbook_id, platform=DashboardPlatform.TABLEAU
            ),
            dashboard_info=dashboard_info,
        )

        self._dashboards[workbook_id] = dashboard

    def _parse_dashboard_upstream(self, workbook: Dict) -> None:
        dashboard = self._dashboards[workbook["vizportalUrlId"]]

        upstream_datasets = [
            self._parse_dataset_id(table) for table in workbook["upstreamTables"]
        ]
        source_datasets = [
            str(dataset_id)
            for dataset_id in upstream_datasets
            if dataset_id is not None
        ]

        if source_datasets:
            dashboard.upstream = DashboardUpstream(source_datasets=source_datasets)

    def _parse_dataset_id(self, table: Dict) -> Optional[EntityId]:
        table_name = table["name"]
        table_schema = table["schema"]
        database_name = table["database"]["name"]

        if None in (table_name, table_schema, database_name):
            return None

        connection_type = table["database"]["connectionType"]
        if connection_type not in connection_type_map:
            # connection type not supported
            return None

        platform = connection_type_map[connection_type]

        if "." in table_name:
            fullname = f"{database_name}.{table_name}"
        else:
            fullname = f"{database_name}.{table_schema}.{table_name}"

        account = (
            self._snowflake_account if platform == DataPlatform.SNOWFLAKE else None
        )

        logger.info(f"dataset id: {fullname} {connection_type} {account}")
        return to_dataset_entity_id(fullname.lower(), platform, account)

    def _parse_chart(self, view: ViewItem) -> Chart:
        # encode preview image raw bytes into data URL
        preview_data_url = None
        try:
            preview_data_url = (
                TableauExtractor._build_preview_data_url(view.preview_image)
                if view.preview_image
                else None
            )
        except Exception as error:
            logger.error(
                f"failed to fetch preview for chart {view.name}, error {error}"
            )

        view_url = self._build_view_url(view.content_url)

        return Chart(title=view.name, url=view_url, preview=preview_data_url)

    _workbook_url_regex = r"(.+\/site\/\w+)\/workbooks\/(\d+)(\/.*)*"

    @staticmethod
    def _parse_workbook_url(workbook_url: str) -> Tuple[str, str]:
        """return base URL containing site ID and the workbook vizportalUrlId"""
        match = re.search(TableauExtractor._workbook_url_regex, workbook_url)
        assert match, f"invalid workbook URL {workbook_url}"

        return match.group(1), match.group(2)

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
