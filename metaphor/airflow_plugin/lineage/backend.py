from typing import TYPE_CHECKING, Collection, Dict, List, Optional

from airflow.configuration import conf

from metaphor.common.event_util import ENTITY_TYPES, EventUtil
from metaphor.common.file_sink import FileSink, FileSinkConfig

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

from airflow.lineage.backend import LineageBackend
from metaphor.models.metadata_change_event import Dataset, DatasetUpstream, EntityType

from metaphor.airflow_plugin.lineage.entity import MetaphorDataset
from metaphor.common.entity_id import EntityId


class MetaphorBackend(LineageBackend):
    def send_lineage(
        self,
        operator: "BaseOperator",
        inlets: Optional[List] = None,
        outlets: Optional[List] = None,
        context: Optional[Dict] = None,
    ) -> None:
        if inlets is None or not MetaphorBackend.is_list_of_dataset(inlets):
            operator.log.error("Not supported inlets")
            return

        if outlets is None or not MetaphorBackend.is_list_of_dataset(outlets):
            operator.log.error("Not supported inlets")
            return

        if context is None:
            return

        if hasattr(operator, "sql"):
            transformation = operator.sql  # type: ignore
        else:
            transformation = None

        upstream = DatasetUpstream(
            source_datasets=[
                str(
                    EntityId(
                        EntityType.DATASET, MetaphorDataset.to_dataset_logical_id(inlet)
                    )
                )
                for inlet in inlets
            ],
            transformation=transformation,
        )

        entities = [
            Dataset(
                entity_type=EntityType.DATASET,
                logical_id=MetaphorDataset.to_dataset_logical_id(outlet),
                upstream=upstream,
            )
            for outlet in outlets
        ]

        # This is necessary to avoid issues with circular imports.
        # from airflow.serialization.serialized_objects import (
        #     SerializedBaseOperator,
        # )
        task: "BaseOperator" = context["task"]
        task_id = task.task_id

        dag: "DAG" = context["dag"]
        dag_id = dag.dag_id

        target_name = f"{task_id}_{dag_id}"

        operator.log.info(target_name)

        MetaphorBackend.sink_event(entities, target_name)
        operator.log.info("Done. Created lineage")

    @staticmethod
    def is_list_of_dataset(source: Optional[List]) -> bool:
        """Check if data is valid"""
        return isinstance(source, list) and (
            len(source) == 0 or isinstance(source[0], MetaphorDataset)
        )

    @staticmethod
    def sink_event(entities: Collection[ENTITY_TYPES], target_name: str) -> None:
        entities = [EventUtil.trim_event(entity) for entity in entities]
        valid_entities = [r for r in entities if EventUtil.validate_message(r)]
        if len(valid_entities) == 0:
            return
        output_directory = conf.get("lineage", "metaphor_output_directory", fallback="")
        file_sink = FileSink(FileSinkConfig(directory=output_directory))
        file_sink.sink_lineage(entities, target_name)
