from typing import List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.custom.query_attributions.config import CustomQueryAttributionsConfig
from metaphor.models.metadata_change_event import MetadataChangeEvent

logger = get_logger()


class CustomQueryAttributionsExtractor(BaseExtractor):
    @staticmethod
    def from_config_file(config_file: str) -> "CustomQueryAttributionsExtractor":
        return CustomQueryAttributionsExtractor(
            CustomQueryAttributionsConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: CustomQueryAttributionsConfig) -> None:
        super().__init__(config, "Custom query attribution connector", None)
        self._config = config

    async def extract(self) -> List[MetadataChangeEvent]:
        logger.info("Fetching custom query attributions from config")
        query_attributions = [
            attributions.to_mce_query_attributions()
            for attributions in self._config.attributions
        ]
        return query_attributions
