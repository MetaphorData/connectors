from typing import List

from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetUpstream,
    MetadataChangeEvent,
)

from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger

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
            source_datasets = set()
            for id in lineage.upstreams:
                source_datasets.add(str(id.to_entity_id()))
                extra_datasets[id.to_entity_id()] = Dataset(
                    logical_id=id.to_logical_id()
                )

            datasets.append(
                Dataset(
                    logical_id=lineage.dataset.to_logical_id(),
                    upstream=DatasetUpstream(source_datasets=list(source_datasets)),
                )
            )

        datasets.extend(extra_datasets.values())

        return datasets
