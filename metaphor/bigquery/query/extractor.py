from typing import Collection

from metaphor.bigquery.query.config import BigQueryQueryRunConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform

logger = get_logger(__name__)


class BigQueryQueryExtractor(BaseExtractor):
    """BigQuery query history extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryQueryExtractor":
        return BigQueryQueryExtractor(
            BigQueryQueryRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: BigQueryQueryRunConfig):
        super().__init__(config, "BigQuery recent queries crawler", Platform.BIGQUERY)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.warn(
            "WARNING: This connector has been merged into 'bigquery' and is no longer needed."
        )
        return []
