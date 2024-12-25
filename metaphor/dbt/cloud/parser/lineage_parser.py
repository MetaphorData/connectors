from typing import Dict, List, Union

from metaphor.common.entity_id import (
    to_dataset_entity_id_from_logical_id,
    to_entity_id_from_virtual_view_logical_id,
    to_metric_entity_id_from_logical_id,
)
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.generated.enums import ResourceNodeType
from metaphor.dbt.cloud.discovery_api.generated.get_lineage import (
    GetLineageEnvironmentAppliedLineageLineageNode as LineageNodeNoParent,
)
from metaphor.dbt.cloud.discovery_api.generated.get_lineage import (
    GetLineageEnvironmentAppliedLineageMacroLineageNode as MacroNode,
)
from metaphor.dbt.cloud.discovery_api.generated.get_lineage import (
    GetLineageEnvironmentAppliedLineageMetricLineageNode as MetricNode,
)
from metaphor.dbt.cloud.discovery_api.generated.get_lineage import (
    GetLineageEnvironmentAppliedLineageModelLineageNode as ModelNode,
)
from metaphor.dbt.cloud.discovery_api.generated.get_lineage import (
    GetLineageEnvironmentAppliedLineageSemanticModelLineageNode as SemanticModelNode,
)
from metaphor.dbt.cloud.discovery_api.generated.get_lineage import (
    GetLineageEnvironmentAppliedLineageSnapshotLineageNode as SnapshotNode,
)
from metaphor.dbt.cloud.discovery_api.generated.get_lineage import (
    GetLineageEnvironmentAppliedLineageTestLineageNode as TestNode,
)
from metaphor.dbt.cloud.discovery_api.generated.input_types import LineageFilter
from metaphor.models.metadata_change_event import (
    Dataset,
    DbtMacro,
    EntityUpstream,
    Metric,
    VirtualView,
)

logger = get_logger()


NodeType = Union[
    LineageNodeNoParent,
    MacroNode,
    MetricNode,
    ModelNode,
    SemanticModelNode,
    SnapshotNode,
    TestNode,
]


class LineageParser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
        metrics: Dict[str, Metric],
    ) -> None:
        self._discovery_api = discovery_api
        self._datasets = datasets
        self._virtual_views = virtual_views
        self._metrics = metrics

    def _parse_metric_lineage(
        self,
        lineage: MetricNode,
    ) -> None:
        metric = self._metrics.get(lineage.unique_id, None)
        if not metric:
            logger.warning(f"Metric not found: {lineage.unique_id}")
            return

        source_metrics = [
            to_metric_entity_id_from_logical_id(self._metrics[n].logical_id)  # type: ignore
            for n in lineage.parent_ids
            if n in self._metrics
        ]
        source_models = [
            to_entity_id_from_virtual_view_logical_id(self._virtual_views[n].logical_id)  # type: ignore
            for n in lineage.parent_ids
            if n.startswith("model.") and n in self._virtual_views
        ]
        # TODO: capture lineage from semantic model to metric

        source_entities = unique_list(
            [str(id) for id in source_metrics + source_models if id]
        )
        if source_entities:
            metric.entity_upstream = EntityUpstream(source_entities=source_entities)

    def _parse_model_lineage(
        self,
        lineage: Union[ModelNode, SnapshotNode],
        macros: Dict[str, DbtMacro],
    ) -> None:
        model = self._virtual_views.get(lineage.unique_id, None)
        if not model:
            logger.warning(f"Model not found: {lineage.unique_id}")
            return

        source_models = [
            to_entity_id_from_virtual_view_logical_id(self._virtual_views[n].logical_id)  # type: ignore
            for n in lineage.parent_ids
            if (n.startswith("model.") or n.startswith("snapshot."))
            and n in self._virtual_views
        ]
        source_datasets = [
            to_dataset_entity_id_from_logical_id(self._datasets[n].logical_id)  # type: ignore
            for n in lineage.parent_ids
            if n.startswith("source.") and n in self._datasets
        ]

        source_entities = unique_list(
            [str(id) for id in source_models + source_datasets if id]
        )
        if source_entities:
            model.entity_upstream = EntityUpstream(source_entities=source_entities)

        depends_on_macros = [
            macros[n]
            for n in lineage.parent_ids
            if n.startswith("macro.") and n in macros
        ]
        if depends_on_macros:
            dbt_model = model.dbt_model
            assert dbt_model, "Virtual view does not have dbt_model"
            dbt_model.macros = (dbt_model.macros or []) + depends_on_macros

    def _parse_lineage(
        self,
        lineage: NodeType,
        macros: Dict[str, DbtMacro],
    ) -> None:
        if isinstance(lineage, LineageNodeNoParent) or not lineage.parent_ids:
            return

        if isinstance(lineage, MetricNode):
            self._parse_metric_lineage(lineage)
            return

        if isinstance(lineage, ModelNode) or isinstance(lineage, SnapshotNode):
            self._parse_model_lineage(lineage, macros)
            return

    def _get_lineage_in_environment(self, environment_id: int) -> List[NodeType]:
        """
        Fetch lineage in a given environment
        """
        filter = LineageFilter(
            types=[
                ResourceNodeType.Source,
                ResourceNodeType.Model,
                ResourceNodeType.SemanticModel,
                ResourceNodeType.Snapshot,
                ResourceNodeType.Metric,
                ResourceNodeType.Macro,
                ResourceNodeType.Test,
            ],
        )
        result = self._discovery_api.get_lineage(environment_id, filter=filter)

        assert result.environment and result.environment.applied
        return result.environment.applied.lineage

    def parse(
        self,
        environment_id: int,
        macros: Dict[str, DbtMacro],
    ) -> None:
        """
        Fetch and parse lineage in a given environment
        """
        lineage_nodes = self._get_lineage_in_environment(environment_id)
        logger.info(
            f"Found {len(lineage_nodes)} lineage nodes in environment {environment_id}"
        )

        for lineage_node in lineage_nodes:
            self._parse_lineage(lineage_node, macros)
