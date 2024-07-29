from typing import Dict, Optional

from metaphor.common.entity_id import EntityId
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_metrics import (
    GetJobRunMetricsJobMetrics as JobMetric,
)
from metaphor.dbt.cloud.parser.common import parse_depends_on
from metaphor.dbt.util import build_metric_docs_url, build_system_tags, init_metric
from metaphor.models.metadata_change_event import (
    DbtMacro,
    DbtMetric,
    EntityUpstream,
    Metric,
    MetricFilter,
    VirtualView,
)


class MetricParser:
    def __init__(
        self,
        metrics: Dict[str, Metric],
        virtual_views: Dict[str, VirtualView],
        docs_base_url: Optional[str],
    ) -> None:
        self._metrics = metrics
        self._virtual_views = virtual_views
        self._docs_base_url = docs_base_url

    def parse(
        self,
        metric: JobMetric,
        source_map: Dict[str, EntityId],
        macro_map: Dict[str, DbtMacro],
    ) -> None:

        metric_entity = init_metric(self._metrics, metric.unique_id)
        metric_entity.dbt_metric = DbtMetric(
            package_name=metric.package_name,
            description=metric.description or None,
            label=metric.label,
            timestamp=metric.timestamp,
            time_grains=metric.time_grains,
            dimensions=metric.dimensions,
            filters=[
                MetricFilter(field=f.field, operator=f.operator, value=f.value)
                for f in metric.filters
            ],
            url=build_metric_docs_url(self._docs_base_url, metric.unique_id),
            sql=metric.expression or metric.sql,
            type=metric.calculation_method or metric.type,
        )
        if metric.tags:
            metric_entity.system_tags = build_system_tags(metric.tags)

        parse_depends_on(
            self._virtual_views,
            metric.depends_on,
            source_map,
            macro_map,
            metric_entity.dbt_metric,
        )

        metric_entity.entity_upstream = EntityUpstream(
            source_entities=metric_entity.dbt_metric.source_models,
        )
