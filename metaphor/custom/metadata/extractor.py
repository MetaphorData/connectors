import json
from typing import List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.custom.metadata.config import CustomMetadataConfig
from metaphor.models.metadata_change_event import (
    CustomMetadata,
    CustomMetadataItem,
    Dataset,
)

logger = get_logger()


class CustomMetadataExtractor(BaseExtractor):
    """Custom metadata extractor"""

    _description = "Custom metadata connector"
    _platform = None

    @staticmethod
    def from_config_file(config_file: str) -> "CustomMetadataExtractor":
        return CustomMetadataExtractor(CustomMetadataConfig.from_yaml_file(config_file))

    def __init__(self, config: CustomMetadataConfig) -> None:
        super().__init__(config)
        self._datasets = config.datasets

    async def extract(self) -> List[Dataset]:
        logger.info("Fetching custom metadata from config")

        datasets = []
        for dataset_metadata in self._datasets:
            dataset = Dataset(logical_id=dataset_metadata.id.to_logical_id())
            datasets.append(dataset)

            dataset.custom_metadata = CustomMetadata(metadata=[])
            for key, value in dataset_metadata.metadata.items():
                dataset.custom_metadata.metadata.append(
                    CustomMetadataItem(key=key, value=json.dumps(value))
                )

        return datasets
