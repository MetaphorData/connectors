from typing import Collection
from warnings import warn

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.snowflake.lineage.config import SnowflakeLineageRunConfig

logger = get_logger()


SUPPORTED_OBJECT_DOMAIN_TYPES = (
    "TABLE",
    "VIEW",
    "MATERIALIZED VIEW",
    "STREAM",
)


class SnowflakeLineageExtractor(BaseExtractor):
    """Snowflake lineage extractor"""

    _description = "Snowflake data lineage crawler"
    _platform = Platform.SNOWFLAKE

    @staticmethod
    def from_config_file(config_file: str) -> "SnowflakeLineageExtractor":
        return SnowflakeLineageExtractor(
            SnowflakeLineageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: SnowflakeLineageRunConfig):
        super().__init__(config)
        warn(
            "Snowflake lineage crawler is deprecated, and is marked for removal in 0.15.0",
            DeprecationWarning,
            stacklevel=2,
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        # Deprecated connector, do nothing!
        return []
