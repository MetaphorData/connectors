import json
from dataclasses import dataclass
from typing import Collection, Dict, List, Optional, Set, Union

import requests
from sql_metadata import Parser

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.metabase.config import MetabaseRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetStructure,
    Chart,
    ChartType,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    DataPlatform,
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
    """Tableau metadata extractor"""

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
        super().__init__(config, "Metabase metadata crawler", Platform.METABASE)
        self._server_url = config.server_url.rstrip("/")
        self._username = config.username
        self._password = config.password

        self._charts: Dict[int, ChartInfo] = {}
        self._dashboards: Dict[int, Dashboard] = {}
        self._databases: Dict[int, DatabaseInfo] = {}
        self._tables: Dict[int, Optional[str]] = {}
        self._session = requests.session()

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

        # fetch all databases
        databases = self._fetch_assets("database", True)
        for database in databases:
            try:
                self._parse_database(database)
            except Exception as ex:
                logger.error(f"error parsing database {database['id']}: {ex}")

        # fetch all cards (charts)
        cards = self._fetch_assets("card")
        for card in cards:
            try:
                self._parse_chart(card)
            except Exception as ex:
                logger.error(f"error parsing card {card['id']}: {ex}")

        # fetch all dashboards
        dashboards = self._fetch_assets("dashboard")
        for dashboard in dashboards:
            try:
                self._parse_dashboard(dashboard)
            except Exception as ex:
                logger.error(f"error parsing dashboard {dashboard['id']}: {ex}")

        return self._dashboards.values()

    def _fetch_assets(self, asset_type: str, withData=False) -> List[Dict]:
        resp = self._session.get(f"{self._server_url}/api/{asset_type}")
        resp.raise_for_status()
        resp_json = resp.json()["data"] if withData else resp.json()

        logger.info(
            f"\nFound {len(resp_json)} {asset_type}s: {[d['name'] for d in resp_json]}"
        )
        logger.debug(json.dumps(resp_json))

        return resp_json

    def _fetch_asset(self, asset_type: str, asset_id: Union[str, int]) -> Dict:
        resp = self._session.get(f"{self._server_url}/api/{asset_type}/{asset_id}")
        resp.raise_for_status()
        resp_json = resp.json()

        logger.debug(json.dumps(resp_json))
        return resp_json

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
        dashboard_details = self._fetch_asset("dashboard", dashboard_id)
        name = dashboard_details["name"]

        cards = dashboard_details.get("ordered_cards", [])
        charts, upstream_datasets = [], set()
        for card in cards:
            card_id = card.get("card_id")
            if card_id is None:
                continue
            if card_id not in self._charts:
                logger.error(f"card {card_id} not found")
            else:
                charts.append(self._charts[card_id].chart)
                upstream_datasets.update(self._charts[card_id].upstream)

        dashboard_info = DashboardInfo(
            title=name,
            description=dashboard_details["description"],
            charts=charts,
        )

        source_info = SourceInfo(
            main_url=f"{self._server_url}/dashboard/{dashboard_id}",
        )

        dashboard_upstream = (
            DashboardUpstream(source_datasets=list(upstream_datasets))
            if upstream_datasets
            else None
        )

        dashboard = Dashboard(
            logical_id=DashboardLogicalID(
                dashboard_id=str(dashboard_id), platform=DashboardPlatform.METABASE
            ),
            structure=AssetStructure(directories=[], name=name),
            dashboard_info=dashboard_info,
            source_info=source_info,
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

        upstream_tables = set()
        dataset_query = card.get("dataset_query", {})
        query_type = dataset_query.get("type")

        if query_type == "query":
            upstream_table_id = dataset_query.get("query", {}).get("source-table", 0)
            dataset_id = self._get_table_by_id(upstream_table_id)
            if dataset_id is not None:
                upstream_tables.add(dataset_id)

        elif query_type == "native":
            dataset_ids = self._parse_native_query(dataset_query)
            upstream_tables.update(dataset_ids)

        else:
            logger.error(f"Unsupported query type {query_type}")

        self._charts[card_id] = ChartInfo(chart, list(upstream_tables))

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

    def _parse_native_query(self, dataset_query: Dict) -> Set[str]:
        try:
            native_query = dataset_query["native"]["query"]
            tables = Parser(native_query).tables

            database_id = dataset_query.get("database", 0)
            database = self._databases.get(database_id)
            if database is None:
                raise ValueError(f"database {database_id} not found")

            dataset_ids = set()
            for table in tables:
                segments = table.count(".") + 1
                if segments == 3:
                    dataset_name = table
                elif segments == 2:
                    dataset_name = f"{database.database}.{table}"
                elif segments == 1:
                    dataset_name = f"{database.database}.{database.schema}.{table}"
                else:
                    raise ValueError(f"invalid table name {table}")

                dataset_ids.add(
                    str(
                        to_dataset_entity_id(
                            dataset_name.replace("`", "").lower(),
                            database.platform,
                            database.account,
                        )
                    )
                )

            return dataset_ids
        except Exception as e:
            logger.error(f"SQL parsing error: {e}, query: {native_query}")
            return set()
