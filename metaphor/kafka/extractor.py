from typing import Collection, Dict, List


from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.kafka.config import KafkaBootstrapServer, KafkaRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    Dataset,
)
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer

logger = get_logger()


class KafkaExtractor(BaseExtractor):
    """Kafka metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "KafkaExtractor":
        return KafkaExtractor(KafkaRunConfig.from_yaml_file(config_file))

    def __init__(self, config: KafkaRunConfig) -> None:
        super().__init__(config, "Kafka metadata crawler", Platform.UNKNOWN) # FIXME

        self._config = config
        self._admin_client = KafkaExtractor.init_admin_client(self._config)
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Kafka")

        Dataset()
        """
    def _extract_record(
        self,
        topic: str,
        topic_detail: Optional[TopicMetadata],
        extra_topic_config: Optional[Dict[str, ConfigEntry]],
    ) -> Iterable[MetadataWorkUnit]:
        logger.debug(f"topic = {topic}")

        AVRO = "AVRO"

        # 1. Create the default dataset snapshot for the topic.
        dataset_name = topic
        platform_urn = make_data_platform_urn(self.platform)
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[Status(removed=False)],  # we append to this list later on
        )

        # 2. Attach schemaMetadata aspect (pass control to SchemaRegistry)
        schema_metadata = self.schema_registry_client.get_schema_metadata(
            topic, platform_urn
        )
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        # 3. Attach browsePaths aspect
        browse_path_str = f"/{self.source_config.env.lower()}/{self.platform}"
        if self.source_config.platform_instance:
            browse_path_str += f"/{self.source_config.platform_instance}"
        browse_path = BrowsePathsClass([browse_path_str])
        dataset_snapshot.aspects.append(browse_path)

        custom_props = self.build_custom_properties(
            topic, topic_detail, extra_topic_config
        )

        # 4. Set dataset's description, tags, ownership, etc, if topic schema type is avro
        description: Optional[str] = None
        if (
            schema_metadata is not None
            and isinstance(schema_metadata.platformSchema, KafkaSchemaClass)
            and schema_metadata.platformSchema.documentSchemaType == AVRO
        ):
            # Point to note:
            # In Kafka documentSchema and keySchema both contains "doc" field.
            # DataHub Dataset "description" field is mapped to documentSchema's "doc" field.

            avro_schema = avro.schema.parse(
                schema_metadata.platformSchema.documentSchema
            )
            description = getattr(avro_schema, "doc", None)
            # set the tags
            all_tags: List[str] = []
            try:
                schema_tags = cast(
                    Iterable[str],
                    avro_schema.other_props.get(
                        self.source_config.schema_tags_field, []
                    ),
                )
                for tag in schema_tags:
                    all_tags.append(self.source_config.tag_prefix + tag)
            except TypeError:
                pass

            if self.source_config.enable_meta_mapping:
                meta_aspects = self.meta_processor.process(avro_schema.other_props)

                meta_owners_aspects = meta_aspects.get(Constants.ADD_OWNER_OPERATION)
                if meta_owners_aspects:
                    dataset_snapshot.aspects.append(meta_owners_aspects)

                meta_terms_aspect = meta_aspects.get(Constants.ADD_TERM_OPERATION)
                if meta_terms_aspect:
                    dataset_snapshot.aspects.append(meta_terms_aspect)

                # Create the tags aspect
                meta_tags_aspect = meta_aspects.get(Constants.ADD_TAG_OPERATION)
                if meta_tags_aspect:
                    all_tags += [
                        tag_association.tag[len("urn:li:tag:") :]
                        for tag_association in meta_tags_aspect.tags
                    ]

            if all_tags:
                dataset_snapshot.aspects.append(
                    mce_builder.make_global_tag_aspect_with_tag_list(all_tags)
                )

        dataset_properties = DatasetPropertiesClass(
            name=topic, customProperties=custom_props, description=description
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # 5. Attach dataPlatformInstance aspect.
        if self.source_config.platform_instance:
            dataset_snapshot.aspects.append(
                DataPlatformInstanceClass(
                    platform=platform_urn,
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                )
            )

        # 6. Emit the datasetSnapshot MCE
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield MetadataWorkUnit(id=f"kafka-{topic}", mce=mce)

        # 7. Add the subtype aspect marking this as a "topic"
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.TOPIC]),
        ).as_workunit()

        domain_urn: Optional[str] = None

        # 8. Emit domains aspect MCPW
        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=dataset_urn,
                domain_urn=domain_urn,
            )
        """
        # Want: topic name & schema (avro or protobuf)
        # schema comes from schema registry, there are more than 1, but let's see what MSK and confluence use
        #       Idea: give one schema id, get one schema back
        # .         in linkedin, topic name == schema id

        


    @staticmethod
    def init_admin_client(config: KafkaRunConfig) -> AdminClient:
        return AdminClient(**config.as_config_dict())

    @staticmethod
    def init_consumer(config: KafkaRunConfig) -> Consumer:
        return Consumer(**config.as_config_dict())
