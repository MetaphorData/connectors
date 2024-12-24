from typing import Dict, List

from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.generated.get_metrics import (
    GetMetricsEnvironmentDefinitionMetricsEdgesNode as Node,
)
from metaphor.dbt.cloud.parser.common import (
    DISCOVERY_API_PAGE_LIMIT,
    update_entity_system_tags,
)
from metaphor.dbt.util import init_metric
from metaphor.models.metadata_change_event import DbtMetric, Metric


class MetricParser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
        metrics: Dict[str, Metric],
        project_explore_base_url: str,
    ) -> None:
        self._discovery_api = discovery_api
        self._metrics = metrics
        self._project_explore_base_url = project_explore_base_url

    def _parse_metric(
        self,
        dbt_metric: Node,
    ) -> None:

        metric = init_metric(self._metrics, dbt_metric.unique_id)
        metric.dbt_metric = DbtMetric(
            package_name=dbt_metric.package_name or None,
            description=dbt_metric.description or None,
            url=f"{self._project_explore_base_url}/{dbt_metric.unique_id}",
            sql=dbt_metric.formula or None,
            type=dbt_metric.type,
            # TODO: add filters and measures
        )
        update_entity_system_tags(metric, dbt_metric.tags or [])

    def _get_metrics_in_environment(self, environment_id: int) -> List[Node]:
        metric_nodes: List[Node] = []
        after = None

        while True:
            result = self._discovery_api.get_metrics(
                environment_id,
                first=DISCOVERY_API_PAGE_LIMIT,
                after=after,
            )
            definition = result.environment.definition
            if not definition or not definition.metrics:
                break
            metric_nodes += [e.node for e in definition.metrics.edges]
            after = definition.metrics.page_info.end_cursor
            if not definition.metrics.page_info.has_next_page:
                break

        return metric_nodes

    def parse(self, environment_id: int) -> None:
        """
        Fetch and parse metrics in a given environment, results are stored in metrics dict.
        """
        metrics = self._get_metrics_in_environment(environment_id)
        for metric in metrics:
            self._parse_metric(metric)
