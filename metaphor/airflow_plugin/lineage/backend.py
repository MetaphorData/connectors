from typing import TYPE_CHECKING, Collection, Dict, List, Optional

from airflow.configuration import conf

from metaphor.common.api_sink import ApiSink, ApiSinkConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.file_sink import FileSink, FileSinkConfig

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

from airflow.lineage.backend import LineageBackend
from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetUpstream,
    EntityType,
    MetadataChangeEvent,
)

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

        task: "BaseOperator" = context["task"]
        task_id = task.task_id

        dag: "DAG" = context["dag"]
        dag_id = dag.dag_id

        target_name = f"{task_id}_{dag_id}"

        operator.log.info(target_name)

        MetaphorBackend.sink_lineage(entities, target_name)
        operator.log.info("Done. Created lineage")

    @staticmethod
    def is_list_of_dataset(source: Optional[List]) -> bool:
        """Check if data is valid"""
        return isinstance(source, list) and (
            len(source) == 0 or isinstance(source[0], MetaphorDataset)
        )

    @staticmethod
    def sink_lineage(
        entities: Collection[MetadataChangeEvent], target_name: str
    ) -> None:
        event_util = EventUtil()
        entities = [event_util.build_event(entity) for entity in entities]

        s3_url = conf.get("lineage", "metaphor_s3_url", fallback="")
        # access_key = conf.get("lineage", "metaphor_aws_access_key_id", fallback="")
        # secreet = conf.get("lineage", "metaphor_aws_secret_access_key", fallback="")
        # session_token = conf.get("lineage", "metaphor_aws_session_token", fallback="")
        ingestion_url = conf.get("lineage", "metaphor_ingestion_url", fallback="")
        ingestion_key = conf.get("lineage", "metaphor_ingestion_key", fallback="")

        if ingestion_key:
            sink = ApiSink(ApiSinkConfig(url=ingestion_url, api_key=ingestion_key))
        else:
            sink = FileSink(FileSinkConfig(directory=s3_url))
        sink.sink(entities)
