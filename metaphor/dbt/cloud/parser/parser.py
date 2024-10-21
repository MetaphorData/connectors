import time
from typing import Dict, Optional, Set

from metaphor.common.logger import get_logger
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.dbt.cloud.client import DbtRun
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_models import (
    GetJobRunModelsJobModels as Model,
)
from metaphor.dbt.cloud.parser.dbt_macro_parser import MacroParser
from metaphor.dbt.cloud.parser.dbt_metric_parser import MetricParser
from metaphor.dbt.cloud.parser.dbt_node_parser import NodeParser
from metaphor.dbt.cloud.parser.dbt_source_parser import SourceParser
from metaphor.dbt.cloud.parser.dbt_test_parser import TestParser
from metaphor.dbt.util import init_virtual_view
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    VirtualView,
)

logger = get_logger()


class Parser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
        config: DbtCloudConfig,
        platform: DataPlatform,
        account: Optional[str],
        project_name: Optional[str],
        docs_base_url: str,
        project_explore_url: str,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
        metrics: Dict[str, Metric],
    ) -> None:
        self._discovery_api = discovery_api
        self._platform = platform
        self._account = account
        self._config = config

        self._datasets: Dict[str, Dataset] = datasets
        self._virtual_views: Dict[str, VirtualView] = virtual_views
        self._metrics: Dict[str, Metric] = metrics
        self._referenced_virtual_views: Set[str] = set()

        self._project_name = project_name
        if self._account and platform == DataPlatform.SNOWFLAKE:
            self._account = normalize_snowflake_account(self._account)

        self._source_parser = SourceParser(
            self._datasets, self._platform, self._account
        )
        self._macro_parser = MacroParser(self._discovery_api)
        self._node_parser = NodeParser(
            self._discovery_api,
            self._config,
            self._platform,
            self._account,
            docs_base_url,
            project_explore_url,
            self._datasets,
            self._virtual_views,
            self._metrics,
        )
        self._test_parser = TestParser(
            self._platform,
            self._account,
            self._virtual_views,
            self._datasets,
        )
        self._metric_parser = MetricParser(
            self._metrics,
            self._virtual_views,
            docs_base_url,
        )

    def _get_source_map(self, run: DbtRun):
        job_run_sources = self._discovery_api.get_job_run_sources(
            run.job_id, run.run_id
        )
        assert job_run_sources.job
        return self._source_parser.parse(job_run_sources.job.sources)

    def _get_macro_map(self, run: DbtRun):

        job_run_macros = self._discovery_api.get_job_run_macros(run.job_id, run.run_id)
        assert job_run_macros.job
        return self._macro_parser.parse(job_run_macros.job.macros)

    def _get_nodes(self, run: DbtRun):
        job_run_models = self._discovery_api.get_job_run_models(run.job_id, run.run_id)
        assert job_run_models.job
        job_run_snapshots = self._discovery_api.get_job_run_snapshots(
            run.job_id, run.run_id
        )
        assert job_run_snapshots.job
        return job_run_models.job.models + job_run_snapshots.job.snapshots

    def _get_tests(self, run: DbtRun):
        job_run_tests = self._discovery_api.get_job_run_tests(run.job_id, run.run_id)
        assert job_run_tests.job
        return job_run_tests.job.tests

    def _get_metrics(self, run: DbtRun):
        job_run_metrics = self._discovery_api.get_job_run_metrics(
            run.job_id, run.run_id
        )
        assert job_run_metrics.job
        return job_run_metrics.job.metrics

    def parse_run(self, run: DbtRun):
        """
        Parses a single job run.
        """
        start = time.time()
        nodes = self._get_nodes(run)
        if not nodes:
            logger.info(f"Nothing to parse for job: {run.job_id}")
            return

        models: Dict[str, Model] = dict()
        for node in nodes:
            init_virtual_view(
                self._virtual_views, node.unique_id, NodeParser.get_node_name(node)
            )
            if self._project_name and node.package_name != self._project_name:
                self._referenced_virtual_views.add(node.unique_id)
            if isinstance(node, Model):
                models[node.unique_id] = node

        source_map = self._get_source_map(run)
        macro_map = self._get_macro_map(run)

        for node in nodes:
            self._node_parser.parse(node, source_map, macro_map)

        for test in self._get_tests(run):
            self._test_parser.parse(test, models)

        for metric in self._get_metrics(run):
            self._metric_parser.parse(metric, source_map, macro_map)

        logger.info(
            f"Fetched job ID: {run.job_id}. Elapsed time: {time.time() - start} secs."
        )
