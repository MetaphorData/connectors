from typing import List

from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetUpstream,
    EntityType,
    MetadataChangeEvent,
)

from metaphor.common.entity_id import EntityId
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list

from .config import ManualLienageConfig

logger = get_logger(__name__)


class ManualLineageExtractor(BaseExtractor):
    """Manual lineage extractor"""

    @staticmethod
    def config_class():
        return ManualLienageConfig

    async def extract(self, config: ManualLienageConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, ManualLineageExtractor.config_class())
        logger.info("Fetching lineage from config")

        # Create a placeholder dataset for each unique upstream dataset
        extra_datasets = {}

        datasets = []
        for lineage in config.lineages:
            source_datasets = []
            for id in lineage.upstreams:
                source_datasets.append(str(id.to_entity_id()))
                extra_datasets[id.to_entity_id()] = Dataset(
                    logical_id=id.to_logical_id()
                )

            datasets.append(
                Dataset(
                    logical_id=lineage.dataset.to_logical_id(),
                    upstream=DatasetUpstream(
                        source_datasets=unique_list(source_datasets)
                    ),
                )
            )

        # Remove existing datasets
        for dataset in datasets:
            extra_datasets.pop(EntityId(EntityType.DATASET, dataset.logical_id), None)

        datasets.extend(extra_datasets.values())

        return datasets
