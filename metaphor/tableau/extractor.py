import base64
import re
from dataclasses import dataclass
from typing import Collection, Dict, List, Optional, Set, Tuple, Union

from metaphor.tableau.workbook import Workbook, get_all_workbooks

try:
    import tableauserverclient as tableau
except ImportError:
    print("Please install metaphor[tableau] extra\n")
    raise
from sqllineage.runner import LineageRunner

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    EntityId,
    to_dataset_entity_id,
    to_dataset_entity_id_from_logical_id,
    to_virtual_view_entity_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetStructure,
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    EntityUpstream,
    SourceInfo,
    SystemContact,
    SystemContacts,
    SystemContactSource,
    SystemTag,
    SystemTags,
    SystemTagSource,
    TableauDatasource,
    TableauField,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from metaphor.tableau.config import PERSONAL_SPACE_PROJECT_NAME, TableauRunConfig
from metaphor.tableau.graphql_utils import fetch_custom_sql_tables
from metaphor.tableau.query import (
    CustomSqlTable,
    DatabaseTable,
    WorkbookQueryResponse,
    connection_type_map,
)

logger = get_logger()


@dataclass
class CustomSqlSource:
    """
    The result class for parsing custom SQL table
    """

    query: str
    platform: DataPlatform
    account: Optional[str]
    sources: List[str]  # source dataset IDs


class TableauExtractor(BaseExtractor):
    """Tableau metadata extractor"""

    _description = "Tableau metadata crawler"
    _platform = Platform.TABLEAU

    @staticmethod
    def from_config_file(config_file: str) -> "TableauExtractor":
        return TableauExtractor(TableauRunConfig.from_yaml_file(config_file))

    @staticmethod
    def _build_base_url(server_url: str, site_name: str) -> str:
        return f"{server_url}/#/site/{site_name}" if site_name else f"{server_url}/#"

    def __init__(self, config: TableauRunConfig):
        super().__init__(config)
        self._server_url = config.server_url
        self._site_name = config.site_name
        self._access_token = config.access_token
        self._user_password = config.user_password
        self._snowflake_account = config.snowflake_account
        self._bigquery_project_name_to_id_map = config.bigquery_project_name_to_id_map
        self._disable_preview_image = config.disable_preview_image
        self._include_personal_space = config.include_personal_space
        self._projects_filter = config.projects_filter
        self._graphql_pagination_size = config.graphql_pagination_size

        self._views: Dict[str, tableau.ViewItem] = {}
        self._projects: Dict[str, List[str]] = {}  # project id -> project hierarchy
        self._datasets: Dict[EntityId, Dataset] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._dashboards: Dict[str, Dashboard] = {}

        self._users: Dict[str, tableau.UserItem] = {}

        # Map workbook id -> project_id
        self._workbook_project: Dict[str, str] = {}

        # The base URL for dashboards, data sources, etc.
        # Use alternative_base_url if provided, otherwise, use server_url as the base
        self._base_url = TableauExtractor._build_base_url(
            config.alternative_base_url or self._server_url, self._site_name
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Tableau")

        tableau_auth: Union[tableau.PersonalAccessTokenAuth, tableau.TableauAuth]
        if self._access_token is not None:
            tableau_auth = tableau.PersonalAccessTokenAuth(
                self._access_token.token_name,
                self._access_token.token_value,
                self._site_name,
            )
        elif self._user_password is not None:
            tableau_auth = tableau.TableauAuth(
                self._user_password.username,
                self._user_password.password,
                self._site_name,
            )
        else:
            raise Exception(
                "Must provide either access token or user password in config"
            )

        server = tableau.Server(self._server_url, use_server_version=True)
        with server.auth.sign_in(tableau_auth):
            workbooks = get_all_workbooks(server, self._graphql_pagination_size)
            self._extract_dashboards(server, workbooks)
            self._extract_datasources(server, workbooks)

        return [
            *self._dashboards.values(),
            *self._virtual_views.values(),
            *self._datasets.values(),
        ]

    def _extract_dashboards(
        self, server: tableau.Server, workbooks: List[Workbook]
    ) -> None:
        # fetch all projects
        projects: List[tableau.ProjectItem] = list(tableau.Pager(server.projects))
        json_dump_to_debug_file([w.__dict__ for w in projects], "projects.json")
        logger.info(
            f"\nThere are {len(projects)} projects on site: {[project.name for project in projects]}"
        )
        self._parse_project_names(projects)

        # fetch all views, with preview image
        views: List[tableau.ViewItem] = list(tableau.Pager(server.views, usage=True))
        json_dump_to_debug_file([v.__dict__ for v in views], "views.json")
        logger.info(
            f"There are {len(views)} views on site: {[view.name for view in views]}\n"
        )
        for view in views:
            if not self._disable_preview_image:
                server.views.populate_preview_image(view)
            if not view.id:
                logger.exception(f"view {view.name} missing id")
                continue
            self._views[view.id] = view

        for workbook in workbooks:
            if workbook.id is not None and workbook.project_id is not None:
                self._workbook_project[workbook.id] = str(workbook.project_id)

            server.workbooks.populate_views(workbook.rest_item, usage=True)

            try:
                self._parse_dashboard(
                    workbook,
                    self._get_system_contacts(server, workbook.rest_item.owner_id),
                )
            except Exception:
                logger.exception(f"failed to parse workbook {workbook.rest_item.name}")

    def _extract_datasources(
        self, server: tableau.Server, workbooks: List[Workbook]
    ) -> None:
        custom_sql_tables = fetch_custom_sql_tables(
            server, self._graphql_pagination_size
        )

        # mapping of datasource to (query, list of upstream dataset IDs)
        datasource_upstream_datasets = {
            datasource_id: custom_sql_source
            for table in custom_sql_tables
            for datasource_id, custom_sql_source in self._parse_custom_sql_table(
                table
            ).items()
        }

        for workbook in workbooks:
            try:
                if not self._should_include_workbook(workbook):
                    logger.info(
                        f"Ignoring datasources from workbook in excluded project: {workbook.project_name}, workbook id = {workbook.id}"
                    )
                    continue
                self._parse_workbook_query_response(
                    server, workbook.graphql_item, datasource_upstream_datasets
                )
            except Exception:
                logger.exception(
                    f"failed to parse workbook {workbook.graphql_item.vizportalUrlId}"
                )

    def _get_system_contacts(
        self, server: tableau.Server, user_id: Optional[str]
    ) -> Optional[SystemContacts]:
        if not user_id:
            return None

        if user_id not in self._users:
            # Cache this to avoid querying the same user
            self._users[user_id] = server.users.get_by_id(user_id)

        system_contact = SystemContact(
            email=self._users[user_id].email,
            system_contact_source=SystemContactSource.TABLEAU,
        )
        return SystemContacts(contacts=[system_contact])

    def _parse_project_names(self, projects: List[tableau.ProjectItem]) -> None:
        for project in projects:
            if project.id:
                self._projects[project.id] = [project.name]

        # second iteration to link child to parent project
        for project in projects:
            if project.id and project.parent_id in self._projects:
                parent_hierarchy = self._projects[project.parent_id]
                self._projects[project.id] = parent_hierarchy + [project.name]

        logger.info(self._projects)

    def _build_asset_full_name_and_structure(
        self,
        asset_name: Optional[str],
        project_id: Optional[str],
        project_name: Optional[str],
    ) -> Tuple[str, AssetStructure]:
        """
        Builds the dashboard or datasource full name <project>.<asset_name> And assetStructure
        Use 'project_id' to find the project full name, if not found, use the given project_name.
        If asset doesn't have project, use only the asset name
        """
        assert asset_name, "missing asset name"
        project_hierarchy = (
            self._projects.get(project_id or "", None) or [project_name]
            if project_name
            else None
        )

        full_name = (
            f"{'.'.join(project_hierarchy)}.{asset_name}"
            if project_hierarchy
            else asset_name
        )
        structure = AssetStructure(
            directories=project_hierarchy or None, name=asset_name
        )
        return full_name, structure

    def _parse_dashboard(
        self, workbook: Workbook, system_contacts: Optional[SystemContacts]
    ) -> None:
        if not self._should_include_workbook(workbook):
            logger.info(
                f"Ignoring dashboard from workbook in excluded project: {workbook.project_name}, workbook id = {workbook.id}"
            )
            return

        rest_workbook = workbook.rest_item
        if not rest_workbook.webpage_url:
            logger.exception(f"workbook {rest_workbook.name} missing webpage_url")
            return

        workbook_id = TableauExtractor._extract_workbook_id(rest_workbook.webpage_url)

        views: List[tableau.ViewItem] = rest_workbook.views
        charts = [self._parse_chart(self._views[view.id]) for view in views if view.id]
        total_views = sum([view.total_views for view in views])

        full_name, structure = self._build_asset_full_name_and_structure(
            rest_workbook.name, rest_workbook.project_id, rest_workbook.project_name
        )

        dashboard_info = DashboardInfo(
            title=full_name,
            description=rest_workbook.description,
            charts=charts,
            view_count=float(total_views),
        )

        source_info = SourceInfo(
            main_url=f"{self._base_url}/workbooks/{workbook_id}",
        )

        dashboard = Dashboard(
            logical_id=DashboardLogicalID(
                dashboard_id=workbook_id, platform=DashboardPlatform.TABLEAU
            ),
            structure=structure,
            dashboard_info=dashboard_info,
            source_info=source_info,
            system_contacts=system_contacts,
        )

        self._dashboards[workbook_id] = dashboard

        if rest_workbook.tags:
            dashboard.system_tags = SystemTags(
                tags=[
                    SystemTag(value=tag, system_tag_source=SystemTagSource.TABLEAU)
                    for tag in sorted(rest_workbook.tags)
                ]
            )

    def _parse_custom_sql_table(
        self, custom_sql_table: CustomSqlTable
    ) -> Dict[str, CustomSqlSource]:
        """
        Parse custom SQL and return mapping of datasource ID to (query, List of upstream dataset IDs)
        """
        platform = connection_type_map.get(custom_sql_table.connectionType)
        if platform is None:
            logger.warning(
                f"Unsupported connection type {custom_sql_table.connectionType} for custom sql table: {custom_sql_table.id}"
            )
            return {}

        account = (
            self._snowflake_account if platform == DataPlatform.SNOWFLAKE else None
        )

        datasource_ids = TableauExtractor._custom_sql_datasource_ids(custom_sql_table)
        if len(datasource_ids) == 0:
            logger.warning(
                f"Missing datasource IDs for custom sql table: {custom_sql_table.id}"
            )
            return {}

        query = custom_sql_table.query
        upstream_datasets = []
        source_tables = []

        try:
            parser = LineageRunner(query)
            source_tables = parser.source_tables

            if len(source_tables) == 0:
                logger.error(
                    f"Unable to extract source tables from custom query for {custom_sql_table.id}"
                )
        except Exception as e:
            logger.error(f"Unable to parse custom query for {custom_sql_table.id}: {e}")

        for source_table in source_tables:
            fullname = str(source_table).lower()
            if fullname.count(".") != 2:
                logger.warning(f"Ignore non-fully qualified source table {fullname}")
                continue

            self._init_dataset(fullname, platform, account, None)
            upstream_datasets.append(
                str(to_dataset_entity_id(fullname, platform, account))
            )

        custom_query_source = CustomSqlSource(
            query=query, platform=platform, account=account, sources=upstream_datasets
        )

        datasource_upstream_datasets = {}
        for datasource_id in datasource_ids:
            datasource_upstream_datasets[datasource_id] = custom_query_source

        return datasource_upstream_datasets

    @staticmethod
    def _custom_sql_datasource_ids(custom_sql_table: CustomSqlTable) -> Set[str]:
        datasource_ids = set()
        for column in custom_sql_table.columnsConnection.nodes:
            for field in column.referencedByFields:
                datasource_ids.add(field.datasource.id)

        return datasource_ids

    def _parse_workbook_query_response(
        self,
        server: tableau.Server,
        workbook: WorkbookQueryResponse,
        datasource_upstream_datasets: Dict[str, CustomSqlSource],
    ) -> None:
        dashboard = self._dashboards[workbook.vizportalUrlId]
        source_virtual_views: List[str] = []
        published_datasources: List[str] = []

        system_tags = SystemTags(tags=[])
        if workbook.tags:
            system_tags.tags = [
                SystemTag(value=tag, system_tag_source=SystemTagSource.TABLEAU)
                for tag in sorted(tag.name for tag in workbook.tags)
            ]

        for published_source in workbook.upstreamDatasources:
            virtual_view_id = str(
                to_virtual_view_entity_id(
                    published_source.luid, VirtualViewType.TABLEAU_DATASOURCE
                )
            )
            if published_source.luid in self._virtual_views:
                # data source already parsed
                source_virtual_views.append(virtual_view_id)
                published_datasources.append(published_source.name)
                continue

            # Use the upstream datasets parsed from custom SQL if available
            custom_sql_source = datasource_upstream_datasets.get(published_source.id)
            # if source_datasets is None or empty from custom SQL, use the upstreamTables of the datasource
            source_datasets = (
                custom_sql_source.sources if custom_sql_source else None
            ) or self._parse_upstream_datasets(
                published_source.upstreamTables, system_tags
            )

            full_name, structure = self._build_asset_full_name_and_structure(
                published_source.name,
                self._workbook_project.get(workbook.luid),
                workbook.projectName,
            )

            system_contacts = self._get_system_contacts(
                server, published_source.owner.luid
            )

            self._virtual_views[published_source.luid] = VirtualView(
                logical_id=VirtualViewLogicalID(
                    type=VirtualViewType.TABLEAU_DATASOURCE, name=published_source.luid
                ),
                structure=structure,
                tableau_datasource=TableauDatasource(
                    name=full_name,
                    description=published_source.description or None,
                    fields=[
                        TableauField(field=f.name, description=f.description or None)
                        for f in published_source.fields
                    ],
                    embedded=False,
                    query=custom_sql_source.query if custom_sql_source else None,
                    source_platform=(
                        custom_sql_source.platform if custom_sql_source else None
                    ),
                    source_dataset_account=(
                        custom_sql_source.account if custom_sql_source else None
                    ),
                    url=f"{self._base_url}/datasources/{published_source.vizportalUrlId}",
                    source_datasets=source_datasets or None,
                ),
                entity_upstream=(
                    EntityUpstream(source_entities=source_datasets)
                    if source_datasets
                    else None
                ),
                system_tags=system_tags,
                system_contacts=system_contacts,
            )
            source_virtual_views.append(virtual_view_id)
            published_datasources.append(published_source.name)

        for embedded_source in workbook.embeddedDatasources:
            if embedded_source.name in published_datasources:
                logger.debug(
                    f"Skip embedded datasource {embedded_source.name} since it's published"
                )
                continue

            virtual_view_id = str(
                to_virtual_view_entity_id(
                    embedded_source.id, VirtualViewType.TABLEAU_DATASOURCE
                )
            )

            # Use the upstream datasets parsed from custom SQL if available
            custom_sql_source = datasource_upstream_datasets.get(embedded_source.id)
            # if source_datasets is None or empty from custom SQL, use the upstreamTables of the datasource
            source_datasets = (
                custom_sql_source.sources if custom_sql_source else None
            ) or self._parse_upstream_datasets(
                embedded_source.upstreamTables, system_tags
            )

            self._virtual_views[embedded_source.id] = VirtualView(
                logical_id=VirtualViewLogicalID(
                    type=VirtualViewType.TABLEAU_DATASOURCE, name=embedded_source.id
                ),
                structure=AssetStructure(
                    directories=[workbook.projectName],
                    name=embedded_source.name,
                ),
                tableau_datasource=TableauDatasource(
                    name=f"{workbook.projectName}.{embedded_source.name}",
                    fields=[
                        TableauField(field=f.name, description=f.description or None)
                        for f in embedded_source.fields
                    ],
                    embedded=True,
                    query=custom_sql_source.query if custom_sql_source else None,
                    source_platform=(
                        custom_sql_source.platform if custom_sql_source else None
                    ),
                    source_dataset_account=(
                        custom_sql_source.account if custom_sql_source else None
                    ),
                    source_datasets=source_datasets or None,
                ),
                entity_upstream=(
                    EntityUpstream(source_entities=source_datasets)
                    if source_datasets
                    else None
                ),
                system_tags=system_tags,
            )
            source_virtual_views.append(virtual_view_id)

        dashboard.entity_upstream = EntityUpstream(source_entities=source_virtual_views)

    def _parse_upstream_datasets(
        self,
        upstreamTables: List[DatabaseTable],
        system_tags: Optional[SystemTags],
    ) -> List[str]:
        upstream_datasets = [
            self._parse_dataset_id(table, system_tags) for table in upstreamTables
        ]
        return list(
            set(
                [
                    str(dataset_id)
                    for dataset_id in upstream_datasets
                    if dataset_id is not None
                ]
            )
        )

    def _parse_dataset_id(
        self, table: DatabaseTable, system_tags: Optional[SystemTags]
    ) -> Optional[EntityId]:
        if (
            not table.name
            or not table.schema_
            or not table.fullName
            or not table.database
        ):
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
        self._init_dataset(fullname, platform, account, system_tags)
        return to_dataset_entity_id(fullname, platform, account)

    def _parse_chart(self, view: tableau.ViewItem) -> Chart:
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

    _workbook_url_regex = r".+\/workbooks\/(\d+)(\/.*)?"

    def _init_dataset(
        self,
        normalized_name: str,
        platform: DataPlatform,
        account: Optional[str],
        system_tags: Optional[SystemTags],
    ) -> Dataset:
        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            name=normalized_name, account=account, platform=platform
        )
        dataset.system_tags = system_tags
        entity_id = to_dataset_entity_id_from_logical_id(dataset.logical_id)
        return self._datasets.setdefault(entity_id, dataset)

    @staticmethod
    def _extract_workbook_id(workbook_url: str) -> str:
        """Extracts the workbook ID from a workbook URL"""
        match = re.search(TableauExtractor._workbook_url_regex, workbook_url)
        assert match, f"invalid workbook URL {workbook_url}"

        return match.group(1)

    def _build_view_url(self, content_url: Optional[str]) -> Optional[str]:
        """
        Builds view URL from the API content_url field.
        content_url is in the form of <workbook>/sheets/<view>, e.g. 'Superstore/sheets/WhatIfForecast'
        """
        if not content_url:
            return None

        workbook, _, view = content_url.split("/")

        return f"{self._base_url}/views/{workbook}/{view}"

    @staticmethod
    def _build_preview_data_url(preview: bytes) -> str:
        return f"data:image/png;base64,{base64.b64encode(preview).decode('ascii')}"

    def _should_include_workbook(self, workbook: Workbook) -> bool:
        if (
            not self._include_personal_space
            and workbook.project_name == PERSONAL_SPACE_PROJECT_NAME
        ):
            return False
        return self._projects_filter.include_project(workbook.project_id)
