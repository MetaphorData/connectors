import json
from dataclasses import dataclass
from itertools import chain
from typing import Collection, Dict, List, Optional, Union

import requests

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.metabase.config import MetabaseRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetStructure,
    Chart,
    ChartQuery,
    ChartType,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DataPlatform,
    EntityUpstream,
    Hierarchy,
    HierarchyInfo,
    HierarchyLogicalID,
    HierarchyType,
    MetabaseCollection,
    SourceInfo,
)

logger = get_logger()


@dataclass()
class ChartInfo:
    chart: Chart
    upstream: List[str]  # list of upstream dataset IDs


@dataclass()
class DatabaseInfo:
    platform: DataPlatform
    database: str
    schema: Optional[str]
    account: Optional[str]


class MetabaseExtractor(BaseExtractor):
    """Metabase metadata extractor"""

    _description = "Metabase metadata crawler"
    _platform = Platform.METABASE

    _chart_type_mapping = dict(
        table=ChartType.TABLE,
        bar=ChartType.BAR,
        line=ChartType.LINE,
        row=ChartType.BAR,
        area=ChartType.AREA,
        pie=ChartType.PIE,
        funnel=ChartType.FUNNEL,
        scatter=ChartType.SCATTER,
        scalar=ChartType.TEXT,
        smartscalar=ChartType.TEXT,
        waterfall=ChartType.WATERFALL,
        map=ChartType.MAP,
        progress=ChartType.TIMELINE,
        combo=ChartType.OTHER,
        gauge=ChartType.OTHER,
        pivot=ChartType.OTHER,
    )

    _db_engine_mapping = {
        "bigquery": DataPlatform.BIGQUERY,
        "bigquery-cloud-sdk": DataPlatform.BIGQUERY,
        "snowflake": DataPlatform.SNOWFLAKE,
        "redshift": DataPlatform.REDSHIFT,
    }

    @staticmethod
    def from_config_file(config_file: str) -> "MetabaseExtractor":
        return MetabaseExtractor(MetabaseRunConfig.from_yaml_file(config_file))

    def __init__(self, config: MetabaseRunConfig):
        super().__init__(config)
        self._server_url = config.server_url.rstrip("/")
        self._username = config.username
        self._password = config.password
        self._session = requests.session()
        self._database_defaults = config.database_defaults
        self._filter = config.filter

        self._databases: Dict[int, DatabaseInfo] = {}
        self._dashboards: Dict[int, Dashboard] = {}
        self._collections: Dict[int, Hierarchy] = {}
        self._tables: Dict[int, Optional[str]] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Metabase")

        # fetch session token
        session_token_resp = requests.post(
            f"{self._server_url}/api/session",
            None,
            {
                "username": self._username,
                "password": self._password,
            },
            timeout=600,  # request timeout 600s
        )
        session_token_resp.raise_for_status()
        session_token = session_token_resp.json()["id"]

        self._session.headers.update(
            {
                "X-Metabase-Session": session_token,
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        # fetch all collections
        collections = self._fetch_assets("collection")
        for collection in collections:
            try:
                self._parse_collection(collection)
            except Exception as ex:
                logger.error(f"error parsing collection {collection['id']}: {ex}")

        # fetch all databases
        databases = self._fetch_assets("database", True)
        for database in databases:
            try:
                self._parse_database(database)
            except Exception as ex:
                logger.error(f"error parsing database {database['id']}: {ex}")

        # fetch all dashboards
        dashboards = self._fetch_assets("dashboard")
        for dashboard in dashboards:
            try:
                self._parse_dashboard(dashboard)
            except Exception as ex:
                logger.error(f"error parsing dashboard {dashboard['id']}: {ex}")

        return list(chain(self._dashboards.values(), self._collections.values()))

    def _fetch_assets(self, asset_type: str, withData=False) -> List[Dict]:
        resp = self._session.get(f"{self._server_url}/api/{asset_type}")
        resp.raise_for_status()
        resp_json = resp.json()["data"] if withData else resp.json()

        logger.info(
            f"\nFound {len(resp_json)} {asset_type}s: {[d['name'] for d in resp_json]}"
        )
        json_dump_to_debug_file(resp_json, f"{asset_type}.json")
        return resp_json

    def _fetch_asset(self, asset_type: str, asset_id: Union[str, int]) -> Dict:
        resp = self._session.get(f"{self._server_url}/api/{asset_type}/{asset_id}")
        resp.raise_for_status()
        resp_json = resp.json()

        json_dump_to_debug_file(resp_json, f"{asset_type}__{asset_id}.json")
        return resp_json

    def _parse_collection(self, collection: Dict) -> None:
        collection_id = collection["id"]

        if collection_id == "root":
            return

        slug = collection.get("slug")
        name = collection.get("name")
        description = collection.get("description")
        location = collection.get("location")

        if not location:
            # We need location to build logical id
            logger.warning(f"invalid collection, dict: {json.dumps(collection)}")
            return

        parent_path = location.split("/")[1:-1]
        logger.debug(
            f"location {location}, parent_path {parent_path}, collection {collection_id}"
        )

        path = parent_path + [str(collection_id)]
        if not self._filter.include_path(path):
            logger.info(f"skipping hierarchy path {path}")
            return

        hierarchy = Hierarchy(
            logical_id=HierarchyLogicalID(
                path=[DashboardPlatform.METABASE.value] + path
            ),
            hierarchy_info=HierarchyInfo(
                description=description,
                type=HierarchyType.METABASE_COLLECTION,
                name=name,
                metabase_collection=MetabaseCollection(
                    name=name,
                    url=f"{self._server_url}/collection/{collection_id}-{slug}",
                ),
            ),
        )

        self._collections[collection_id] = hierarchy

    def _parse_database(self, database: Dict) -> None:
        database_id = database["id"]
        platform = self._db_engine_mapping.get(database["engine"])
        details = database.get("details")
        default_schema = next(
            (
                dd.default_schema
                for dd in self._database_defaults
                if dd.id == database["id"]
            ),
            None,
        )

        if details is None:
            # not able to get connection details, possibly due to lack of Admin permission
            return

        if platform == DataPlatform.SNOWFLAKE:
            self._databases[database_id] = DatabaseInfo(
                platform,
                details.get("db"),
                default_schema,
                details.get("account"),
            )
        elif platform == DataPlatform.BIGQUERY:
            self._databases[database_id] = DatabaseInfo(
                platform,
                details.get("project-id"),
                default_schema or details.get("dataset-id"),
                None,
            )
        elif platform == DataPlatform.REDSHIFT:
            self._databases[database_id] = DatabaseInfo(
                platform, details.get("db"), default_schema, None
            )
        # platform not in _db_engine_mapping are not supported

    def _parse_dashboard(self, dashboard: Dict) -> None:
        dashboard_id = dashboard["id"]

        # need to fetch the dashboard details, which contains the cards info
        dashboard_details = self._fetch_asset("dashboard", dashboard_id)
        name = dashboard_details["name"]

        dashboard_collection = dashboard_details.get("collection")
        collection = (
            self._collections.get(dashboard_collection.get("id"))
            if dashboard_collection
            else None
        )

        path = collection.logical_id.path[1:] if collection else []
        if not self._filter.include_path(path):
            logger.info(f"skipping collection path {path}")
            return

        cards = dashboard_details.get("dashcards", [])
        charts, upstream_datasets = [], set()
        for card in cards:
            chart_info = self._parse_chart(card["card"])
            if chart_info is not None:
                charts.append(chart_info.chart)
                upstream_datasets.update(chart_info.upstream)

        dashboard_info = DashboardInfo(
            title=name,
            description=dashboard_details["description"],
            charts=charts,
        )

        source_info = SourceInfo(
            main_url=f"{self._server_url}/dashboard/{dashboard_id}",
        )

        entity_upstream = (
            EntityUpstream(source_entities=list(upstream_datasets))
            if upstream_datasets
            else None
        )

        dashboard = Dashboard(
            logical_id=DashboardLogicalID(
                dashboard_id=str(dashboard_id), platform=DashboardPlatform.METABASE
            ),
            structure=AssetStructure(
                directories=path,
                name=name,
            ),
            dashboard_info=dashboard_info,
            source_info=source_info,
            entity_upstream=entity_upstream,
        )

        self._dashboards[dashboard_id] = dashboard

    def _parse_chart(self, card: Dict) -> Optional[ChartInfo]:
        if "id" not in card or "name" not in card:
            return None

        card_id = card["id"]
        chart = Chart(
            title=card["name"],
            id=str(card_id),
            description=card["description"],
            chart_type=self._chart_type_mapping.get(card.get("display", "")),
            url=f"{self._server_url}/card/{card_id}",
        )

        upstream_tables = set()
        dataset_query = card.get("dataset_query", {})
        query_type = dataset_query.get("type")

        if query_type == "query":
            upstream_table_id = dataset_query.get("query", {}).get("source-table", 0)
            dataset_id = self._get_table_by_id(upstream_table_id)
            if dataset_id is not None:
                upstream_tables.add(dataset_id)

        elif query_type == "native":
            chart.query = self._parse_native_query(dataset_query)

        else:
            logger.error(f"Unsupported query type {query_type}")

        return ChartInfo(chart, list(upstream_tables))

    def _get_table_by_id(self, table_id: int) -> Optional[str]:
        if table_id in self._tables:
            return self._tables[table_id]

        # fetch table detail
        table_json = self._fetch_asset("table", table_id)

        schema = table_json.get("schema")
        name = table_json.get("name")
        database_id = table_json.get("db", {}).get("id")

        database = self._databases.get(database_id)
        if database is None:
            logger.warning(f"database {database_id} not found")
            self._tables[table_id] = None
            return None

        dataset_id = str(
            to_dataset_entity_id(
                dataset_normalized_name(
                    database.database, schema or database.schema, name
                ),
                database.platform,
                database.account,
            )
        )
        logger.info(
            f"table {table_id} {dataset_id} : {database.database}.{schema or database.schema}.{name}, {database.platform}, {database.account}"
        )

        self._tables[table_id] = dataset_id
        return dataset_id

    def _parse_native_query(self, dataset_query: Dict) -> ChartQuery:
        native_query = dataset_query["native"]["query"]

        database_id = dataset_query.get("database", 0)
        database = self._databases.get(database_id)
        if database is None:
            raise ValueError(f"database {database_id} not found")

        return ChartQuery(
            query=native_query,
            platform=database.platform,
            account=database.account,
            default_database=database.database,
            default_schema=database.schema,
        )
