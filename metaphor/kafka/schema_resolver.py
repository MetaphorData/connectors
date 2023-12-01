from typing import Dict, List

from confluent_kafka.schema_registry import RegisteredSchema, SchemaRegistryClient

from metaphor.common.logger import get_logger
from metaphor.kafka.config import KafkaConfig, KafkaSubjectNameStrategy
from metaphor.kafka.schema_parsers.avro_parser import AvroParser
from metaphor.kafka.schema_parsers.protobuf_parser import ProtobufParser
from metaphor.models.metadata_change_event import DatasetSchema, SchemaType

logger = get_logger()


class SchemaResolver:
    def __init__(self, config: KafkaConfig) -> None:
        self._schema_registry_client = self.init_schema_registry_client(config)
        self._topic_naming_strategies = config.topic_naming_strategies
        self._default_subject_name_strategy = config.default_subject_name_strategy
        self._known_subjects = self._schema_registry_client.get_subjects()

    def _resolve_topic_to_subjects(self, topic: str, is_key_schema: bool) -> List[str]:
        """
        Returns the list of subjects that relates to the topic.
        """
        subject_key_suffix: str = "key" if is_key_schema else "value"
        key = f"{topic}-{subject_key_suffix}"

        records, subject_name_strategy = self._resolve_topic_naming_strategy(topic)
        if subject_name_strategy is KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY:
            if not records:
                logger.warning(
                    f"Cannot find record for topic {topic} with name strategy = RECORD_NAME_STRATEGY"
                )
                return []
            resolved_subject_names = [
                f"{record}-{subject_key_suffix}"
                for record in records
                if f"{record}-{subject_key_suffix}" in self._known_subjects
            ]
            if not resolved_subject_names:
                logger.warning(
                    f"No schema subject exist for topic {topic} with name strategy = RECORD_NAME_STRATEGY, records = {records}"
                )
            return resolved_subject_names

        if subject_name_strategy is KafkaSubjectNameStrategy.TOPIC_RECORD_NAME_STRATEGY:
            if records:
                resolved_subject_names = [
                    f"{topic}-{record}-{subject_key_suffix}"
                    for record in records
                    if f"{topic}-{record}-{subject_key_suffix}" in self._known_subjects
                ]
                if not resolved_subject_names:
                    logger.warning(
                        f"No schema subject exist for topic {topic} with name strategy = TOPIC_RECORD_NAME_STRATEGY, records = {records}"
                    )
                return resolved_subject_names
            # If no record is found, just gotta take whatever subject that starts with
            # `topic` and ends with `subject_key_suffix`.

        subjects = []
        for subject in self._known_subjects:
            if (
                subject_name_strategy is KafkaSubjectNameStrategy.TOPIC_NAME_STRATEGY
                and key == subject
            ):
                subjects.append(subject)
            if (
                subject_name_strategy
                is KafkaSubjectNameStrategy.TOPIC_RECORD_NAME_STRATEGY
                and subject.startswith(topic + "-")
                and subject.endswith("-" + subject_key_suffix)
            ):
                subjects.append(subject)
        return subjects

    def get_dataset_schemas(
        self, topic: str, all_versions: bool = False
    ) -> Dict[str, DatasetSchema]:
        value_subjects = self._resolve_topic_to_subjects(topic, is_key_schema=False)
        key_subjects = self._resolve_topic_to_subjects(topic, is_key_schema=True)

        dataset_schemas: Dict[str, DatasetSchema] = {}

        # TODO: get the mapping from key subjects to value subjects
        # For now just concat them
        subjects = key_subjects + value_subjects
        for subject_name in subjects:
            if not all_versions:
                registered_schemas = [
                    self._schema_registry_client.get_latest_version(subject_name)
                ]
            else:
                registered_schemas = [
                    self._schema_registry_client.get_version(subject_name, version)
                    for version in self._schema_registry_client.get_versions(
                        subject_name
                    )
                ]

            for registered_schema in registered_schemas:
                dataset_schema = DatasetSchema(
                    schema_type=SchemaType(registered_schema.schema.schema_type),
                    raw_schema=registered_schema.schema.schema_str,
                )

                assert dataset_schema.raw_schema is not None
                if dataset_schema.schema_type is SchemaType.AVRO:
                    dataset_schema.fields = AvroParser.parse(
                        dataset_schema.raw_schema, subject_name
                    )
                elif dataset_schema.schema_type is SchemaType.PROTOBUF:
                    dataset_schema.fields = ProtobufParser.parse(
                        dataset_schema.raw_schema, subject_name
                    )
                elif dataset_schema.schema_type is SchemaType.JSON:
                    logger.warning("Parsing JSON schema is not supported yet")

                dataset_schemas[
                    SchemaResolver.to_dataset_schema_key(registered_schema)
                ] = dataset_schema

        return dataset_schemas

    @staticmethod
    def to_dataset_schema_key(registered_schema: RegisteredSchema) -> str:
        return f"{registered_schema.schema_id}_{registered_schema.version}"

    @staticmethod
    def init_schema_registry_client(config: KafkaConfig) -> SchemaRegistryClient:
        return SchemaRegistryClient(config.schema_registry_conf)

    @staticmethod
    def build_resolver(config: KafkaConfig) -> "SchemaResolver":
        return SchemaResolver(config)

    def _resolve_topic_naming_strategy(self, topic: str):
        """
        Resolves the topic to a tuple of records and SubjectNameStrategy.
        If the topic isn't in `self._topic_naming_strategies`, returns an empty list and the default SubjectNameStrategy.
        """
        strat = self._topic_naming_strategies.get(topic)
        records = []
        if strat is not None:
            records = strat.records
            if strat.override_subject_name_strategy is not None:
                return records, strat.override_subject_name_strategy
        return records, self._default_subject_name_strategy
