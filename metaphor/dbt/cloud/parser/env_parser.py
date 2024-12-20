import time
from typing import Dict, Optional

from metaphor.common.logger import get_logger
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.parser.lineage_parser import LineageParser
from metaphor.dbt.cloud.parser.macro_parser import MacroParser
from metaphor.dbt.cloud.parser.metric_parser import MetricParser
from metaphor.dbt.cloud.parser.model_parser import ModelParser
from metaphor.dbt.cloud.parser.source_parser import SourceParser
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    VirtualView,
)

logger = get_logger()


class EnvironmentParser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
        config: DbtCloudConfig,
        platform: DataPlatform,
        account: Optional[str],
        project_explore_url: str,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
        metrics: Dict[str, Metric],
    ) -> None:
        self._discovery_api = discovery_api
        self._config = config
        self._platform = platform
        self._account = (
            normalize_snowflake_account(account)
            if account and platform == DataPlatform.SNOWFLAKE
            else account
        )
        self._project_explore_base_url = project_explore_url

        self._datasets: Dict[str, Dataset] = datasets
        self._virtual_views: Dict[str, VirtualView] = virtual_views
        self._metrics: Dict[str, Metric] = metrics

    def parse(self, environment_id: int):
        """
        Fetch and parse a single environment, including nodes and lineage.
        """
        start = time.time()

        source_parser = SourceParser(
            self._discovery_api, self._datasets, self._platform, self._account
        )
        source_parser.parse(environment_id)

        metric_parser = MetricParser(
            self._discovery_api, self._metrics, self._project_explore_base_url
        )
        metric_parser.parse(environment_id)

        macro_parser = MacroParser(self._discovery_api)
        macro_map = macro_parser.parse(environment_id)

        model_parser = ModelParser(
            self._discovery_api,
            self._config,
            self._platform,
            self._account,
            self._project_explore_base_url,
            self._datasets,
            self._virtual_views,
        )
        model_parser.parse(environment_id)

        lineage_parser = LineageParser(
            self._discovery_api, self._datasets, self._virtual_views, self._metrics
        )
        lineage_parser.parse(environment_id, macro_map)

        logger.info(
            f"Fetched environment {environment_id}. Elapsed time: {time.time() - start} secs."
        )
