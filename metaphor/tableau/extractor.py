import base64
import json
import re
import traceback
from typing import Collection, Dict, List, Optional, Tuple

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

from metaphor.common.entity_id import (
    EntityId,
    to_dataset_entity_id,
    to_virtual_view_entity_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    DataPlatform,
    SourceInfo,
    TableauDatasource,
    TableauField,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from metaphor.tableau.config import TableauRunConfig
from metaphor.tableau.query import (
    DatabaseTable,
    WorkbookQueryResponse,
    connection_type_map,
    workbooks_graphql_query,
)

logger = get_logger(__name__)


class TableauExtractor(BaseExtractor):
    """Tableau metadata extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.TABLEAU

    def description(self) -> str:
        return "Tableau metadata crawler"

    def __init__(self):
        self._base_url: Optional[str] = None
        self._views: Dict[str, ViewItem] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._dashboards: Dict[str, Dashboard] = {}
        self._disable_preview_image = False
        self._snowflake_account: Optional[str] = None
        self._bigquery_project_name_to_id_map: Dict[str, str] = dict()

    @staticmethod
    def config_class():
        return TableauRunConfig

    async def extract(self, config: TableauRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, TableauExtractor.config_class())

        logger.info("Fetching metadata from Tableau")

        self._disable_preview_image = config.disable_preview_image
        self._snowflake_account = config.snowflake_account
        self._bigquery_project_name_to_id_map = config.bigquery_project_name_to_id_map

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
            views: List[ViewItem] = list(Pager(server.views, usage=True))
            logger.info(
                f"There are {len(views)} views on site: {[view.name for view in views]}\n"
            )
            for item in views:
                logger.debug(json.dumps(item.__dict__, default=str))
                if not self._disable_preview_image:
                    server.views.populate_preview_image(item)
                self._views[item.id] = item

            # fetch all workbooks
            workbooks: List[WorkbookItem] = list(Pager(server.workbooks))
            logger.info(
                f"\nThere are {len(workbooks)} work books on site: {[workbook.name for workbook in workbooks]}"
            )
            for item in workbooks:
                server.workbooks.populate_views(item, usage=True)
                logger.debug(json.dumps(item.__dict__, default=str))

                try:
                    self._parse_dashboard(item)
                except Exception as error:
                    traceback.print_exc()
                    logger.error(f"failed to parse workbook {item.name}, error {error}")

            # fetch workbook related info from Metadata GraphQL API
            resp = server.metadata.query(workbooks_graphql_query)
            resp_data = resp["data"]
            for item in resp_data["workbooks"]:
                try:
                    workbook = WorkbookQueryResponse.parse_obj(item)
                    self._parse_workbook_query_response(workbook)
                except Exception as error:
                    logger.exception(
                        f"failed to parse workbook {item['vizportalUrlId']}, error {error}"
                    )

        return [*self._dashboards.values(), *self._virtual_views.values()]

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
            charts=charts,
            view_count=float(total_views),
        )

        source_info = SourceInfo(
            main_url=workbook.webpage_url,
        )

        dashboard = Dashboard(
            logical_id=DashboardLogicalID(
                dashboard_id=workbook_id, platform=DashboardPlatform.TABLEAU
            ),
            dashboard_info=dashboard_info,
            source_info=source_info,
        )

        self._dashboards[workbook_id] = dashboard

    def _parse_workbook_query_response(self, workbook: WorkbookQueryResponse) -> None:
        dashboard = self._dashboards[workbook.vizportalUrlId]
        source_virtual_views: List[str] = []
        published_datasources: List[str] = []

        for source in workbook.upstreamDatasources:
            virtual_view_id = str(
                to_virtual_view_entity_id(
                    source.luid, VirtualViewType.TABLEAU_DATASOURCE
                )
            )
            if source.luid in self._virtual_views:
                # data source already parsed
                source_virtual_views.append(virtual_view_id)
                published_datasources.append(source.name)
                continue

            source_datasets = self._parse_upstream_datasets(source.upstreamTables)

            self._virtual_views[source.luid] = VirtualView(
                logical_id=VirtualViewLogicalID(
                    type=VirtualViewType.TABLEAU_DATASOURCE, name=source.luid
                ),
                tableau_datasource=TableauDatasource(
                    name=f"{workbook.projectName}.{source.name}",
                    description=source.description or None,
                    fields=[
                        TableauField(field=f.name, description=f.description or None)
                        for f in source.fields
                    ],
                    embedded=False,
                    url=f"{self._base_url}/datasources/{source.vizportalUrlId}",
                    source_datasets=source_datasets or None,
                ),
            )
            source_virtual_views.append(virtual_view_id)
            published_datasources.append(source.name)

        for source in workbook.embeddedDatasources:
            if source.name in published_datasources:
                logger.debug(
                    f"Skip embedded datasource {source.name} since it's published"
                )
                continue

            virtual_view_id = str(
                to_virtual_view_entity_id(source.id, VirtualViewType.TABLEAU_DATASOURCE)
            )

            source_datasets = self._parse_upstream_datasets(source.upstreamTables)

            self._virtual_views[source.id] = VirtualView(
                logical_id=VirtualViewLogicalID(
                    type=VirtualViewType.TABLEAU_DATASOURCE, name=source.id
                ),
                tableau_datasource=TableauDatasource(
                    name=f"{workbook.projectName}.{source.name}",
                    fields=[
                        TableauField(field=f.name, description=f.description or None)
                        for f in source.fields
                    ],
                    embedded=True,
                    source_datasets=source_datasets or None,
                ),
            )
            source_virtual_views.append(virtual_view_id)

        dashboard.upstream = DashboardUpstream(
            source_virtual_views=source_virtual_views
        )

    def _parse_upstream_datasets(
        self, upstreamTables: List[DatabaseTable]
    ) -> List[str]:
        upstream_datasets = [self._parse_dataset_id(table) for table in upstreamTables]
        return list(
            set(
                [
                    str(dataset_id)
                    for dataset_id in upstream_datasets
                    if dataset_id is not None
                ]
            )
        )

    def _parse_dataset_id(self, table: DatabaseTable) -> Optional[EntityId]:
        if None in (table.name, table.schema, table.fullName, table.database):
            return None

        database_name = table.database.name
        connection_type = table.database.connectionType
        if connection_type not in connection_type_map:
            # connection type not supported
            return None

        platform = connection_type_map[connection_type]

        # if table fullname contains three segments, use it as dataset name
        if table.fullName.count(".") == 2:
            fullname = table.fullName
        else:
            # use BigQuery project ID to replace project name, to be consistent with the BigQuery crawler
            if platform == DataPlatform.BIGQUERY:
                if database_name in self._bigquery_project_name_to_id_map:
                    database_name = self._bigquery_project_name_to_id_map[database_name]
                else:
                    # use project name as database name, may not match with BigQuery crawler
                    logger.warning(
                        f"BigQuery project name {database_name} not defined in config 'bigquery_project_name_to_id_map'"
                    )

            # if table name has two segments, then it contains "schema" and "table_name"
            if "." in table.name:
                fullname = f"{database_name}.{table.name}"
            else:
                fullname = f"{database_name}.{table.schema_}.{table.name}"

        fullname = (
            fullname.replace("[", "")
            .replace("]", "")
            .replace("`", "")
            .replace("'", "")
            .replace('"', "")
            .lower()
        )

        account = (
            self._snowflake_account if platform == DataPlatform.SNOWFLAKE else None
        )

        logger.debug(f"dataset id: {fullname} {connection_type} {account}")
        return to_dataset_entity_id(fullname, platform, account)

    def _parse_chart(self, view: ViewItem) -> Chart:
        # encode preview image raw bytes into data URL
        preview_data_url = None
        try:
            preview_data_url = (
                TableauExtractor._build_preview_data_url(view.preview_image)
                if not self._disable_preview_image and view.preview_image
                else None
            )
        except Exception as error:
            logger.error(
                f"Failed to build preview data URL for {view.name}, error {error}"
            )

        view_url = self._build_view_url(view.content_url)

        return Chart(title=view.name, url=view_url, preview=preview_data_url)

    _workbook_url_regex = r"(.+)\/workbooks\/(\d+)(\/.*)?"

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
