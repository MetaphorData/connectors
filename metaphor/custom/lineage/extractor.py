from typing import List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import EntityId
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list
from metaphor.custom.lineage.config import CustomLineageConfig
from metaphor.models.metadata_change_event import (
    Dataset,
    EntityType,
    EntityUpstream,
    MetadataChangeEvent,
)

logger = get_logger()


class CustomLineageExtractor(BaseExtractor):
    """Custom lineage extractor"""

    _description = "Custom data lineage connector"
    _platform = None

    @staticmethod
    def from_config_file(config_file: str) -> "CustomLineageExtractor":
        return CustomLineageExtractor(CustomLineageConfig.from_yaml_file(config_file))

    def __init__(self, config: CustomLineageConfig) -> None:
        super().__init__(config)
        self._lineages = config.lineages

    async def extract(self) -> List[MetadataChangeEvent]:
        logger.info("Fetching lineage from config")

        # Create a placeholder dataset for each unique upstream dataset
        extra_datasets = {}

        datasets = []
        for lineage in self._lineages:
            source_datasets = []
            for id in lineage.upstreams:
                source_datasets.append(str(id.to_entity_id()))
                extra_datasets[id.to_entity_id()] = Dataset(
                    logical_id=id.to_logical_id()
                )

            unique_datasets = unique_list(source_datasets)
            datasets.append(
                Dataset(
                    logical_id=lineage.dataset.to_logical_id(),
                    entity_upstream=EntityUpstream(source_entities=unique_datasets),
                )
            )

        # Remove existing datasets
        for dataset in datasets:
            extra_datasets.pop(EntityId(EntityType.DATASET, dataset.logical_id), None)

        datasets.extend(extra_datasets.values())

        return datasets
