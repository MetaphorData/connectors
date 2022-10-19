from typing import Collection

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.snowflake.usage.config import SnowflakeUsageRunConfig

logger = get_logger(__name__)


class SnowflakeUsageExtractor(BaseExtractor):
    """Snowflake usage metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "SnowflakeUsageExtractor":
        return SnowflakeUsageExtractor(
            SnowflakeUsageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: SnowflakeUsageRunConfig):
        super().__init__(
            config, "Snowflake usage statistics crawler", Platform.SNOWFLAKE
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.warn(
            "WARNING: This connector has been merged into 'snowflake' and is no longer needed."
        )
        return []
