from dataclasses import dataclass
from typing import Dict, List, Optional

import requests
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

from metaphor.common.entity_id import dataset_fullname, to_dataset_entity_id
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.metabase.config import MetabaseRunConfig

logger = get_logger(__name__)


@dataclass()
class ChartInfo:
    chart: Chart
    upstream: List[int]  # list of upstream table IDs


@dataclass()
class DatabaseInfo:
    platform: DataPlatform
    database: str
    schema: Optional[str]
    account: Optional[str]


class MetabaseExtractor(BaseExtractor):
    """Tableau metadata extractor"""

    def __init__(self):
        self._server_url = ""
        self._session = None

        # mapping of card id to ChartInfo
        self._charts: Dict[int, ChartInfo] = {}

        # mapping of dashboard id to Dashboard
        self._dashboards: Dict[int, Dashboard] = {}

        # mapping of database id to DatabaseInfo
        self._databases: Dict[int, DatabaseInfo] = {}

        # mapping of table id to dataset entity ID string
        self._tables: Dict[int, str] = {}

    @staticmethod
    def config_class():
        return MetabaseRunConfig

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
        "bigquery-cloud-sdk": DataPlatform.BIGQUERY,
        "snowflake": DataPlatform.SNOWFLAKE,
        "redshift": DataPlatform.REDSHIFT,
    }

    async def extract(self, config: MetabaseRunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, MetabaseExtractor.config_class())

        logger.info("Fetching metadata from Metabase")

        self._server_url = config.server_url
        if self._server_url.endswith("/"):
            self._server_url = self._server_url[:-1]

        # fetch session token
        session_token_resp = requests.post(
            f"{self._server_url}/api/session",
            None,
            {
                "username": config.username,
                "password": config.password,
            },
        )
        session_token_resp.raise_for_status()
        session_token = session_token_resp.json()["id"]

        self._session = requests.session()
        self._session.headers.update(
            {
                "X-Metabase-Session": session_token,
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        # fetch all databases
        all_databases_resp = self._session.get(f"{config.server_url}/api/database")
        all_databases_resp.raise_for_status()
        databases = all_databases_resp.json()["data"]
        logger.info(
            f"Found {len(databases)} databases: {[d['name'] for d in databases]}\n"
        )
        for database in databases:
            try:
                self._parse_database(database)
            except Exception as ex:
                logger.error(f"error parsing database {database['id']}", ex)

        # fetch all cards (charts)
        all_cards_resp = self._session.get(f"{config.server_url}/api/card")
        all_cards_resp.raise_for_status()
        cards = all_cards_resp.json()
        logger.info(f"Found {len(cards)} cards: {[d['name'] for d in cards]}\n")
        for card in cards:
            try:
                self._parse_chart(card)
            except Exception as ex:
                logger.error(f"error parsing card {card['id']}", ex)

        # fetch all dashboards
        all_dashboards_resp = self._session.get(f"{config.server_url}/api/dashboard")
        all_dashboards_resp.raise_for_status()
        dashboards = all_dashboards_resp.json()
        # logger.info(json.dumps(dashboards))
        logger.info(
            f"Found {len(dashboards)} dashboards: {[d['name'] for d in dashboards]}\n"
        )
        for dashboard in dashboards:
            try:
                self._parse_dashboard(dashboard)
            except Exception as ex:
                logger.error(f"error parsing dashboard {dashboard['id']}", ex)

        return [EventUtil.build_dashboard_event(d) for d in self._dashboards.values()]

    def _parse_database(self, database: Dict) -> None:
        database_id = database["id"]
        platform = self._db_engine_mapping.get(database["engine"])
        details = database.get("details")
        if details is None:
            # not able to get connection details, possibly due to lack of Admin permission
            return

        if platform == DataPlatform.SNOWFLAKE:
            self._databases[database_id] = DatabaseInfo(
                platform, details.get("db"), None, details.get("account")
            )
        elif platform == DataPlatform.BIGQUERY:
            self._databases[database_id] = DatabaseInfo(
                platform, details.get("project-id"), details.get("dataset-id"), None
            )
        elif platform == DataPlatform.REDSHIFT:
            self._databases[database_id] = DatabaseInfo(
                platform, details.get("db"), None, None
            )
        # platform not in _db_engine_mapping are not supported

    def _parse_dashboard(self, dashboard: Dict) -> None:
        dashboard_id = dashboard["id"]

        # need to fetch the dashboard details, which contains the cards info
        dashboard_resp = self._session.get(
            f"{self._server_url}/api/dashboard/{dashboard_id}"
        )
        dashboard_resp.raise_for_status()
        dashboard_details = dashboard_resp.json()

        cards = dashboard_details.get("ordered_cards", [])
        charts, table_ids = [], set()
        for card in cards:
            card_id = card.get("card_id")
            if card_id is None:
                continue
            if card_id not in self._charts:
                logger.error(f"card {card_id} not found")
            else:
                charts.append(self._charts[card_id].chart)
                table_ids.update(self._charts[card_id].upstream)

        dashboard_info = DashboardInfo(
            title=dashboard_details["name"],
            description=dashboard_details["description"],
            charts=charts,
            url=f"{self._server_url}/dashboard/{dashboard_id}",
        )

        upstream_datasets = [
            self._tables[table_id] for table_id in table_ids if table_id in self._tables
        ]
        dashboard_upstream = (
            DashboardUpstream(source_datasets=upstream_datasets)
            if upstream_datasets
            else None
        )

        dashboard = Dashboard(
            logical_id=DashboardLogicalID(
                dashboard_id=str(dashboard_id), platform=DashboardPlatform.METABASE
            ),
            dashboard_info=dashboard_info,
            upstream=dashboard_upstream,
        )

        self._dashboards[dashboard_id] = dashboard

    def _parse_chart(self, card: Dict) -> None:
        card_id = card["id"]
        chart = Chart(
            title=card["name"],
            description=card["description"],
            chart_type=self._chart_type_mapping.get(card.get("display", "")),
            url=f"{self._server_url}/card/{card_id}",
        )

        upstream_tables = []
        dataset_query = card.get("dataset_query", {})
        query_type = dataset_query.get("type")
        if query_type == "query":
            upstream_table_id = dataset_query.get("query", {}).get("source-table")
            if upstream_table_id is not None:
                self._get_table_by_id(upstream_table_id)
                upstream_tables.append(upstream_table_id)

        # TODO: parse native (raw) query

        self._charts[card_id] = ChartInfo(chart, upstream_tables)

    def _get_table_by_id(self, table_id: int) -> None:
        if table_id in self._tables:
            return

        table_resp = self._session.get(f"{self._server_url}/api/table/{table_id}")
        table_resp.raise_for_status()
        table_json = table_resp.json()

        schema = table_json.get("schema")
        name = table_json.get("name")
        database_id = table_json.get("db", {}).get("id")

        database = self._databases.get(database_id)
        if database is None:
            logger.warning(f"database {database_id} not found")
            return

        dataset_id = to_dataset_entity_id(
            dataset_fullname(database.database, schema or database.schema, name),
            database.platform,
            database.account,
        )
        logger.info(
            f"table {table_id} {dataset_id} : {database.database}.{schema or database.schema}.{name}, {database.platform}, {database.account}"
        )
        self._tables[table_id] = str(dataset_id)
