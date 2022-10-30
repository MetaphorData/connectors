from typing import Collection

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.query.config import RedshiftQueryRunConfig

logger = get_logger()


class RedshiftQueryExtractor(PostgreSQLExtractor):
    """Redshift query extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftQueryExtractor":
        return RedshiftQueryExtractor(
            RedshiftQueryRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: RedshiftQueryRunConfig):
        super().__init__(
            config,
            "Redshift recent queries crawler",
            Platform.REDSHIFT,
            DataPlatform.REDSHIFT,
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.warn(
            "WARNING: This connector has been merged into 'redshift' and is no longer needed."
        )
        return []
