from datetime import datetime
from typing import TYPE_CHECKING, Collection, Dict, List, Optional

from airflow.configuration import conf

from metaphor.common.api_sink import ApiSink, ApiSinkConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.file_sink import FileSink, FileSinkConfig
from metaphor.common.storage import S3StorageConfig

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

from airflow.lineage.backend import LineageBackend
from metaphor.models.metadata_change_event import Dataset, DatasetUpstream, EntityType
from pydantic.dataclasses import dataclass

from metaphor.airflow_plugin.lineage.entity import MetaphorDataset
from metaphor.common.entity_id import EntityId

INGESTION_API_MODE = "ingestion-api"
S3_MODE = "s3"


@dataclass
class MetaphorBackendConfig:
    mode: str
    ingestion_url: Optional[str] = None
    ingestion_key: Optional[str] = None
    s3_url: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    assume_role_arn: Optional[str] = None

    @staticmethod
    def from_config() -> "MetaphorBackendConfig":
        mode = conf.get("lineage", "metaphor_backend_mode", fallback=None)
        s3_url = conf.get("lineage", "metaphor_s3_url", fallback=None)
        aws_access_key_id = conf.get(
            "lineage", "metaphor_aws_access_key_id", fallback=None
        )
        aws_secret_access_key = conf.get(
            "lineage", "metaphor_aws_secret_access_key", fallback=None
        )
        assume_role_arn = conf.get("lineage", "metaphor_assume_role_arn", fallback=None)
        ingestion_url = conf.get("lineage", "metaphor_ingestion_url", fallback=None)
        ingestion_key = conf.get("lineage", "metaphor_ingestion_key", fallback=None)

        return MetaphorBackendConfig(
            mode=mode,
            ingestion_url=ingestion_url,
            ingestion_key=ingestion_key,
            s3_url=s3_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            assume_role_arn=assume_role_arn,
        )


class MetaphorError(Exception):
    pass


class MetaphorBackend(LineageBackend):
    def __init__(self) -> None:
        super().__init__()

        # Try to load config when initialize Airflow
        _ = MetaphorBackendConfig.from_config()

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

        config = MetaphorBackendConfig.from_config()
        entities = MetaphorBackend._populate_lineage(operator, inlets, outlets)
        lineage_name = MetaphorBackend._populate_lineage_name(context)

        operator.log.info(f"Lineage name: ${lineage_name}")

        try:
            MetaphorBackend.sink_lineage(config, entities, lineage_name)
            operator.log.info("Done. Created lineage")
        except MetaphorError as ex:
            operator.log.error(ex)

    @staticmethod
    def _populate_lineage_name(context: Dict) -> str:
        task: "BaseOperator" = context["task"]
        task_id = task.task_id

        dag: "DAG" = context["dag"]
        dag_id = dag.dag_id

        execution_date: datetime = context["execution_date"]
        timestamp = int(datetime.timestamp(execution_date))

        lineage_name = f"{timestamp}_{dag_id}_{task_id}"

        return lineage_name

    @staticmethod
    def _populate_lineage(
        operator: "BaseOperator",
        inlets: List[MetaphorDataset],
        outlets: List[MetaphorDataset],
    ) -> Collection[Dataset]:
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

        return entities

    @staticmethod
    def is_list_of_dataset(source: Optional[List]) -> bool:
        """Check if data is valid"""
        return isinstance(source, list) and (
            len(source) == 0 or isinstance(source[0], MetaphorDataset)
        )

    @staticmethod
    def sink_lineage(
        config: MetaphorBackendConfig,
        entities: Collection[Dataset],
        target_name: str,
    ) -> None:
        event_util = EventUtil()
        entities = [event_util.build_event(entity) for entity in entities]

        if config.mode == INGESTION_API_MODE:
            sink = ApiSink(
                ApiSinkConfig(url=config.ingestion_url, api_key=config.ingestion_key)
            )
        else:
            if config.s3_url is None:
                raise MetaphorError("s3_url is not set")
            directory = f'{config.s3_url.rstrip("/")}/{target_name}'
            print(directory)
            sink = FileSink(
                FileSinkConfig(
                    directory=directory,
                    assume_role_arn=config.assume_role_arn,
                    s3_auth_config=S3StorageConfig(
                        aws_access_key_id=config.aws_access_key_id,
                        aws_secret_access_key=config.aws_secret_access_key,
                    ),
                )
            )
        sink.sink(entities)
