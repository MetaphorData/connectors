import re
from datetime import datetime, timedelta
from typing import Collection, Dict, List

from dateutil import parser

try:
    from pycarlo.core import Client, Session
except ImportError:
    print("Please install metaphor[monte_carlo] extra\n")
    raise

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    normalize_full_dataset_name,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataMonitor,
    DataMonitorSeverity,
    DataMonitorStatus,
    DataMonitorTarget,
    DataPlatform,
    DataQualityProvider,
    Dataset,
    DatasetDataQuality,
    DatasetLogicalID,
)
from metaphor.monte_carlo.config import MonteCarloRunConfig

logger = get_logger()

assets_base_url = "https://getmontecarlo.com/assets"
monitors_base_url = "https://getmontecarlo.com/monitors"
alerts_base_url = "https://getmontecarlo.com/alerts"


monitor_status_map = {
    "SNOOZED": DataMonitorStatus.UNKNOWN,
    "PAUSE": DataMonitorStatus.UNKNOWN,
    "SUCCESS": DataMonitorStatus.PASSED,
    "ERROR": DataMonitorStatus.ERROR,
    "IN_PROGRESS": DataMonitorStatus.UNKNOWN,
    "NO_STATUS": DataMonitorStatus.UNKNOWN,
    "MISCONFIGURED": DataMonitorStatus.ERROR,
    "IN_TRAINING": DataMonitorStatus.UNKNOWN,
}

monitor_severity_map = {
    "P1": DataMonitorSeverity.HIGH,
    "P2": DataMonitorSeverity.MEDIUM,
    "P3": DataMonitorSeverity.LOW,
}

alert_severity_map = {
    "SEV_1": DataMonitorSeverity.HIGH,
    "SEV_2": DataMonitorSeverity.MEDIUM,
    "SEV_3": DataMonitorSeverity.LOW,
    "SEV_4": DataMonitorSeverity.LOW,
}

connection_type_platform_map = {
    "BIGQUERY": DataPlatform.BIGQUERY,
    "REDSHIFT": DataPlatform.REDSHIFT,
    "SNOWFLAKE": DataPlatform.SNOWFLAKE,
}


class MonteCarloExtractor(BaseExtractor):
    """MonteCarlo metadata extractor"""

    _description = "Monte Carlo metadata crawler"
    _platform = Platform.MONTE_CARLO

    @staticmethod
    def from_config_file(config_file: str) -> "MonteCarloExtractor":
        return MonteCarloExtractor(MonteCarloRunConfig.from_yaml_file(config_file))

    def __init__(self, config: MonteCarloRunConfig):
        super().__init__(config)
        self._account = (
            normalize_snowflake_account(config.snowflake_account)
            if config.snowflake_account
            else None
        )
        self._ignored_errors = config.ignored_errors

        self._treat_unhandled_anomalies_as_errors = (
            config.treat_unhandled_anomalies_as_errors
        )
        self._anomalies_lookback_days = config.anomalies_lookback_days

        self._client = Client(
            session=Session(mcd_id=config.api_key_id, mcd_token=config.api_key_secret)
        )

        self._mcon_platform_map: Dict[str, DataPlatform] = {}

        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Monte Carlo")

        self._fetch_tables()
        self._fetch_monitors()

        if self._treat_unhandled_anomalies_as_errors:
            self._fetch_alerts()

        return self._datasets.values()

    def _fetch_monitors(self) -> None:
        """Fetch all monitors

        See https://apidocs.getmontecarlo.com/#query-getMonitors
        """

        offset = 0
        limit = 200

        monitors = []
        while True:
            resp = self._client(
                """
                query getMonitors($offset: Int, $limit: Int) {
                  getMonitors(offset: $offset, limit: $limit) {
                    uuid
                    name
                    description
                    entityMcons
                    priority
                    monitorStatus
                    monitorFields
                    creatorId
                    prevExecutionTime
                    exceptions
                  }
                }
                """,
                {"offset": offset, "limit": limit},
            )

            logger.info(f"Querying getMonitors with offset {offset}")
            monitors.extend(resp["get_monitors"])
            if len(resp["get_monitors"]) < limit:
                break

            offset += limit

        logger.info(f"Fetched {len(monitors)} monitors")
        json_dump_to_debug_file(monitors, "getMonitors.json")
        self._parse_monitors(monitors)

    def _fetch_alerts(self) -> None:
        """Fetch all alerts

        See https://apidocs.getmontecarlo.com/#query-getAlerts
        """

        batch_size = 200
        end_cursor = None
        created_after = datetime.now() - timedelta(days=self._anomalies_lookback_days)
        create_before = datetime.now()

        result: List[Dict] = []

        while True:
            logger.info(f"Querying getAlerts after {end_cursor} ({len(result)} tables)")
            resp = self._client(
                """
                query getAlerts($first: Int, $after: String, $createdAfter: DateTime!, $createdBefore: DateTime!) {
                  getAlerts(first: $first
                            after: $after
                            filter: {
                                include:{
                                    types: [ANOMALIES]
                                }
                            }
                            createdTime: {before: $createdBefore, after: $createdAfter}
                  ) {
                    edges {
                        node {
                            id
                            title
                            type
                            status
                            severity
                            priority
                            owner {
                                email
                            }
                            createdTime
                            tables {
                                mcon
                            }
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                  }
                }
                """,
                {
                    "first": batch_size,
                    "after": end_cursor,
                    "createdAfter": created_after.isoformat(),
                    "createdBefore": create_before.isoformat(),
                },
            )

            nodes = [edge["node"] for edge in resp["get_alerts"]["edges"]]
            result.extend(nodes)

            if not resp["get_alerts"]["page_info"]["has_next_page"]:
                break

            end_cursor = resp["get_alerts"]["page_info"]["end_cursor"]

        logger.info(f"Fetched {len(result)} alerts")
        json_dump_to_debug_file(result, "getAlerts.json")

        self._parse_alerts(result)

    def _fetch_tables(self) -> None:
        """Fetch all tables

        See https://apidocs.getmontecarlo.com/#query-getTables
        """

        batch_size = 500
        end_cursor = None
        result: List[Dict] = []

        while True:
            logger.info(f"Querying getTables after {end_cursor} ({len(result)} tables)")
            resp = self._client(
                """
                query getTables($first: Int, $after: String) {
                  getTables(first: $first after: $after) {
                    edges {
                        node {
                            mcon
                            warehouse {
                                connectionType
                            }
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                  }
                }
                """,
                {"first": batch_size, "after": end_cursor},
            )

            nodes = [edge["node"] for edge in resp["get_tables"]["edges"]]
            result.extend(nodes)

            if not resp["get_tables"]["page_info"]["has_next_page"]:
                break

            end_cursor = resp["get_tables"]["page_info"]["end_cursor"]

        logger.info(f"Fetched {len(result)} tables")
        json_dump_to_debug_file(result, "getTables.json")

        for node in result:
            mcon = node["mcon"]
            connection_type = node["warehouse"]["connection_type"]
            platform = connection_type_platform_map.get(connection_type)
            if platform is None:
                logger.warning(
                    f"Unsupported connection type: {connection_type} for {mcon}"
                )
            else:
                self._mcon_platform_map[mcon] = platform

    def _parse_monitors(self, monitors) -> None:
        for monitor in monitors:
            uuid = monitor["uuid"]
            monitor_severity = monitor_severity_map.get(
                monitor["priority"], DataMonitorSeverity.UNKNOWN
            )

            # MC monitor exceptions is a single string
            exceptions = (
                [monitor["exceptions"]] if monitor["exceptions"] is not None else None
            )

            data_monitor = DataMonitor(
                title=monitor["name"],
                description=monitor["description"],
                owner=monitor["creatorId"],
                status=self._parse_monitor_status(monitor),
                severity=monitor_severity,
                url=f"{monitors_base_url}/{uuid}",
                last_run=parser.parse(monitor["prevExecutionTime"]),
                targets=[
                    DataMonitorTarget(column=field.upper())
                    for field in monitor["monitorFields"] or []
                ],
                exceptions=exceptions,
            )

            if monitor["entityMcons"] is None:
                logger.info(f"Skipping monitors not linked to any entities: {uuid}")
                continue

            for mcon in monitor["entityMcons"]:
                platform = self._mcon_platform_map.get(mcon)
                if platform is None:
                    logger.warning(f"Unable to determine platform for {mcon}")
                    continue

                name = self._extract_dataset_name(mcon)
                dataset = self._init_dataset(name, platform)
                dataset.data_quality.url = f"{assets_base_url}/{mcon}/custom-monitors"
                dataset.data_quality.monitors.append(data_monitor)

    def _parse_alerts(self, alerts) -> None:
        for alert in alerts:
            id = alert["id"]

            if alert["tables"] is None or len(alert["tables"]) == 0:
                logger.info(f"Skipping alert not linked to any tables: {id}")
                continue

            # Filter out alerts with status (e.g. "ACKNOWLEDGED", "EXPECTED")
            if alert["status"] is not None:
                continue

            # Alerts triggered by anomalies do not have any inherited priority.
            # Derive it from assigned severity instead.
            alert_severity = alert_severity_map.get(
                alert["severity"], DataMonitorSeverity.UNKNOWN
            )

            owner = None
            if alert["owner"] is not None and alert["owner"]["email"] is not None:
                owner = alert["owner"]["email"]

            data_monitor = DataMonitor(
                title=alert["title"],
                description=alert["title"],
                owner=owner,
                status=DataMonitorStatus.ERROR,
                severity=alert_severity,
                url=f"{alerts_base_url}/{id}",
                last_run=parser.parse(alert["createdTime"]),
                targets=[],
            )

            for table in alert.get("tables", []):

                mcon = table["mcon"]
                platform = self._mcon_platform_map.get(mcon)
                if platform is None:
                    logger.warning(f"Unable to determine platform for {mcon}")
                    continue

                name = self._extract_dataset_name(mcon)
                dataset = self._init_dataset(name, platform)
                dataset.data_quality.monitors.append(data_monitor)

    def _parse_monitor_status(self, monitor: Dict):
        status = monitor_status_map.get(
            monitor["monitorStatus"], DataMonitorStatus.UNKNOWN
        )

        # Change status to UNKNOWN if the error message is to be ignored
        message = (
            monitor.get("exceptions", "") or ""
        )  # monitor["exceptions"] is default to None
        if status == DataMonitorStatus.ERROR:
            for ignored_error in self._ignored_errors:
                if re.match(ignored_error, message) is not None:
                    return DataMonitorStatus.UNKNOWN

        return status

    @staticmethod
    def _extract_dataset_name(mcon: str) -> str:
        """Extract dataset name form MCON"""

        # MCON has the following format:
        # MCON++{account_uuid}++{resource_uuid}++table++{db:schema.table}
        _, _, _, _, obj_name = mcon.split("++")
        return normalize_full_dataset_name(obj_name.replace(":", ".", 1))

    def _init_dataset(self, normalized_name: str, platform: DataPlatform) -> Dataset:
        account = self._account if platform == DataPlatform.SNOWFLAKE else None

        logical_id = DatasetLogicalID(
            name=normalized_name,
            platform=platform,
            account=account,
        )
        dataset_id = str(to_dataset_entity_id_from_logical_id(logical_id))

        if dataset_id not in self._datasets:
            self._datasets[dataset_id] = Dataset(
                dataset_id=dataset_id,
                logical_id=logical_id,
                data_quality=DatasetDataQuality(
                    provider=DataQualityProvider.MONTE_CARLO, monitors=[]
                ),
            )

        return self._datasets[dataset_id]
