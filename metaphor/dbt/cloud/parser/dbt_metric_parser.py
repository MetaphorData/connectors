import json
from typing import Dict, Optional

from metaphor.common.entity_id import EntityId
from metaphor.common.utils import dedup_lists
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_metrics import (
    GetJobRunMetricsJobMetrics as JobMetric,
)
from metaphor.dbt.cloud.parser.common import parse_depends_on, update_entity_system_tags
from metaphor.dbt.util import build_metric_docs_url, init_metric
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
        job_metric: JobMetric,
        source_map: Dict[str, EntityId],
        macro_map: Dict[str, DbtMacro],
    ) -> None:

        metric = init_metric(self._metrics, job_metric.unique_id)
        MetricFilter.__hash__ = lambda filter: hash(json.dumps(filter.to_dict()))  # type: ignore
        metric.dbt_metric = DbtMetric(
            package_name=job_metric.package_name or None,
            description=job_metric.description or None,
            label=job_metric.label,
            timestamp=job_metric.timestamp,
            time_grains=job_metric.time_grains,
            dimensions=job_metric.dimensions,
            filters=dedup_lists(
                metric.dbt_metric.filters if metric.dbt_metric else [],
                [
                    MetricFilter(field=f.field, operator=f.operator, value=f.value)
                    for f in job_metric.filters
                ],
            ),
            url=build_metric_docs_url(self._docs_base_url, job_metric.unique_id),
            sql=job_metric.expression or job_metric.sql,
            type=job_metric.calculation_method or job_metric.type,
        )
        update_entity_system_tags(metric, job_metric.tags or [])

        parse_depends_on(
            self._virtual_views,
            job_metric.depends_on,
            source_map,
            macro_map,
            metric.dbt_metric,
        )

        metric.entity_upstream = EntityUpstream(
            source_entities=dedup_lists(
                (
                    metric.entity_upstream.source_entities
                    if metric.entity_upstream
                    else []
                ),
                metric.dbt_metric.source_models,
            )
        )
