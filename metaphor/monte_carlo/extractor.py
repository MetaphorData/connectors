import traceback
from typing import Collection, Dict

from metaphor.common.snowflake import normalize_snowflake_account

try:
    from pycarlo.core import Client, Session
except ImportError:
    print("Please install metaphor[monte_carlo] extra\n")
    raise

from dateutil import parser

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    normalize_full_dataset_name,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataMonitor,
    DataMonitorSeverity,
    DataMonitorStatus,
    DataMonitorTarget,
    DataQualityProvider,
    Dataset,
    DatasetDataQuality,
    DatasetLogicalID,
)
from metaphor.monte_carlo.config import MonteCarloRunConfig

logger = get_logger()

monitors_url_prefix = "https://getmontecarlo.com/monitors"

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


class MonteCarloExtractor(BaseExtractor):
    """MonteCarlo metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "MonteCarloExtractor":
        return MonteCarloExtractor(MonteCarloRunConfig.from_yaml_file(config_file))

    def __init__(self, config: MonteCarloRunConfig):
        super().__init__(config, "Monte Carlo metadata crawler", Platform.TABLEAU)
        self._data_platform = config.data_platform
        self._account = (
            normalize_snowflake_account(config.snowflake_account)
            if config.snowflake_account
            else None
        )
        self._client = Client(
            session=Session(mcd_id=config.api_key_id, mcd_token=config.api_key_secret)
        )

        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from MonteCarlo")

        self._fetch_monitors()

        return self._datasets.values()

    def _fetch_monitors(self) -> None:
        # fetch all monitors
        try:
            monitors = self._client(
                """
                {
                  getMonitors {
                    uuid
                    name
                    description
                    entities
                    severity
                    monitorStatus
                    monitorFields
                    creatorId
                    prevExecutionTime
                  }
                }
                """
            )

            logger.info(f"Fetched {len(monitors['get_monitors'])} monitors")
            self._parse_monitors(monitors)
        except Exception as error:
            traceback.print_exc()
            logger.error(f"failed to get all monitors, error {error}")

    def _parse_monitors(self, monitors) -> None:
        for monitor in monitors["get_monitors"]:
            monitor_severity = DataMonitorSeverity.UNKNOWN
            if monitor["severity"]:
                try:
                    monitor_severity = DataMonitorSeverity(
                        monitor["severity"].toUpper()
                    )
                except Exception:
                    logger.warning(f"Unknown severity {monitor['severity']}")

            data_monitor = DataMonitor(
                title=monitor["name"],
                description=monitor["description"],
                owner=monitor["creatorId"],
                status=monitor_status_map.get(
                    monitor["monitorStatus"], DataMonitorStatus.UNKNOWN
                ),
                severity=monitor_severity,
                url=f"{monitors_url_prefix}/{monitor['uuid']}",
                last_run=parser.parse(monitor["prevExecutionTime"]),
                targets=[
                    DataMonitorTarget(column=field)
                    for field in monitor["monitorFields"] or []
                ],
            )

            target_datasets = [
                self._convert_dataset_name(entity) for entity in monitor["entities"]
            ]
            for target in target_datasets:
                dataset = self._init_dataset(target)
                dataset.data_quality.monitors.append(data_monitor)

    @staticmethod
    def _convert_dataset_name(entity: str) -> str:
        """entity name format is <db>:<schema>.<table>"""
        return normalize_full_dataset_name(entity.replace(":", ".", 1))

    def _init_dataset(self, normalized_name: str) -> Dataset:
        logical_id = DatasetLogicalID(
            name=normalized_name,
            platform=self._data_platform,
            account=self._account,
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
