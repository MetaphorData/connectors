from typing import Collection, List, Optional

from confluent_kafka.admin import AdminClient

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.filter import TopicFilter
from metaphor.common.logger import get_logger
from metaphor.kafka.config import KafkaConfig
from metaphor.kafka.schema_resolver import SchemaResolver
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    EntityType,
    SchemaType,
)

logger = get_logger()


DEFAULT_FILTER: TopicFilter = TopicFilter(
    excludes={
        "_schemas",
        "__consumer_offsets",
    }
)


class KafkaExtractor(BaseExtractor):
    """Kafka metadata extractor"""

    _description = "Kafka metadata crawler"
    _platform = Platform.KAFKA

    @staticmethod
    def from_config_file(config_file: str) -> "KafkaExtractor":
        return KafkaExtractor(KafkaConfig.from_yaml_file(config_file))

    def __init__(self, config: KafkaConfig) -> None:
        super().__init__(config)
        self._config = config
        self._filter = config.filter.normalize().merge(DEFAULT_FILTER)
        self._admin_client = KafkaExtractor.init_admin_client(self._config)
        self._resolver = SchemaResolver.build_resolver(config)
        self._datasets: List[Dataset] = []

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Kafka")

        cluster_metadata = self._admin_client.list_topics()
        if cluster_metadata.topics is None:
            raise ValueError("Cannot find any topic")
        for topic in cluster_metadata.topics.keys():
            if self._filter.include_topic(topic):
                schemas = self._resolver.get_dataset_schemas(
                    topic, all_versions=False
                )  # No need for lineage for now
                if not schemas:
                    logger.warning(f"Cannot find schema subject for topic {topic}")
                    self._datasets.append(
                        self._init_dataset(
                            topic,
                            None,
                            DatasetSchema(schema_type=SchemaType.SCHEMALESS),
                        )
                    )
                self._datasets.extend(
                    [
                        self._init_dataset(topic, key, dataset_schema)
                        for key, dataset_schema in schemas.items()
                    ]
                )

        return self._datasets

    def _init_dataset(
        self,
        topic: str,
        dataset_schema_key: Optional[str],
        dataset_schema: DatasetSchema,
    ) -> Dataset:
        return Dataset(
            entity_type=EntityType.DATASET,
            logical_id=DatasetLogicalID(
                platform=DataPlatform.KAFKA,
                name=topic
                + ("" if dataset_schema_key is None else f"_{dataset_schema_key}"),
            ),
            schema=dataset_schema,
            structure=DatasetStructure(
                table=topic,
            ),
        )

    @staticmethod
    def init_admin_client(config: KafkaConfig) -> AdminClient:
        # FIXME how do we get this to break & exit if we cannot connect?
        return AdminClient(config.admin_conf)
