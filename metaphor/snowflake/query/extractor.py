from typing import Collection

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.snowflake.query.config import SnowflakeQueryRunConfig

logger = get_logger()


class SnowflakeQueryExtractor(BaseExtractor):
    """Snowflake query extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SnowflakeQueryExtractor":
        return SnowflakeQueryExtractor(
            SnowflakeQueryRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: SnowflakeQueryRunConfig):
        super().__init__(config, "Snowflake recent queries crawler", Platform.SNOWFLAKE)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.warn(
            "WARNING: This connector has been merged into 'snowflake' and is no longer needed."
        )
        return []
