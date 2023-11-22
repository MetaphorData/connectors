from typing import Collection, List

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.kafka.config import KafkaConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    SchemaType,
)

logger = get_logger()


SYSTEM_GENERATED_TOPICS = {
    "_schemas",
    "__consumer_offsets",
}


class KafkaExtractor(BaseExtractor):
    """Kafka metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "KafkaExtractor":
        return KafkaExtractor(KafkaConfig.from_yaml_file(config_file))

    def __init__(self, config: KafkaConfig) -> None:
        super().__init__(config, "Kafka metadata crawler", Platform.KAFKA)

        self._config = config
        self._admin_client = KafkaExtractor.init_admin_client(self._config)
        self._schema_registry_client = KafkaExtractor.init_schema_registry_client(
            self._config
        )
        self._datasets: List[Dataset] = []

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Kafka")

        cluster_metadata = self._admin_client.list_topics()
        subjects = self._schema_registry_client.get_subjects()
        if cluster_metadata.topics is None:
            raise ValueError("Cannot find any topic")
        for topic in cluster_metadata.topics.keys():
            if topic not in SYSTEM_GENERATED_TOPICS:
                logger.info(f"Topic: {topic}")
                name = topic
                schema_type = SchemaType.SCHEMALESS
                raw_schema = None
                if topic in subjects:
                    # If there's a subject in schema registry equal to the topic, that's
                    # the schema we're looking for.
                    registered_schema = self._schema_registry_client.get_latest_version(
                        topic
                    )  # TODO: lineage / version?
                    schema_type = SchemaType[
                        registered_schema.schema.schema_type
                    ]  # It's either AVRO, JSON or PROTOBUF
                    raw_schema = registered_schema.schema.schema_str
                    name += f"_{registered_schema.version}"
                self._datasets.append(
                    Dataset(
                        logical_id=DatasetLogicalID(
                            platform=DataPlatform.KAFKA,
                            name=name,
                        ),
                        schema=DatasetSchema(
                            raw_schema=raw_schema,
                            schema_type=schema_type,
                        ),
                    )
                )

        return self._datasets

    @staticmethod
    def init_admin_client(config: KafkaConfig) -> AdminClient:
        # FIXME how do we get this to break & exit if we cannot connect?
        return AdminClient(config.as_config_dict())

    @staticmethod
    def init_schema_registry_client(config: KafkaConfig) -> SchemaRegistryClient:
        return SchemaRegistryClient({"url": config.schema_registry_url})
