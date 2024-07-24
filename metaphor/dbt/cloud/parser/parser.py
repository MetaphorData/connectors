from typing import Dict, Set

from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.dbt.cloud.client import DbtRun
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.parser.macro_parser import MacroParser
from metaphor.dbt.cloud.parser.node_parser import NodeParser
from metaphor.dbt.cloud.parser.source_parser import SourceParser
from metaphor.dbt.config import DbtRunConfig
from metaphor.dbt.util import init_virtual_view
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    VirtualView,
)


class Parser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
        config: DbtRunConfig,
        platform: DataPlatform,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
        metrics: Dict[str, Metric],
        referenced_virtual_views: Set[str],
    ) -> None:
        self._discovery_api = discovery_api
        self._platform = platform
        self._config = config
        self._datasets = datasets
        self._virtual_views = virtual_views
        self._metrics = metrics
        self._referenced_virtual_views = referenced_virtual_views

        self._account = config.account
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
            self._datasets,
            self._virtual_views,
            self._metrics,
            self._referenced_virtual_views,
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

    def parse_run(self, run: DbtRun):
        source_map = self._get_source_map(run)
        macro_map = self._get_macro_map(run)

        nodes = self._get_nodes(run)

        for node in nodes:
            init_virtual_view(
                self._virtual_views, node.unique_id, NodeParser._get_node_name(node)
            )

        for node in nodes:
            self._node_parser.parse(node, source_map, macro_map)

        # for _, test in job.tests:
        #     self._parse_test(test, run_results, models)

        # for _, metric in job.metrics:
        #     self._parse_metric(metric, source_map, macro_map)
