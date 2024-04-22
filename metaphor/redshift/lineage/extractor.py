from typing import Collection

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.lineage.config import RedshiftLineageRunConfig

logger = get_logger()


class RedshiftLineageExtractor(PostgreSQLExtractor):
    """Redshift lineage metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftLineageExtractor":
        return RedshiftLineageExtractor(
            RedshiftLineageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: RedshiftLineageRunConfig):
        super().__init__(config)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.warning("This connector has been deprecated and will be removed soon")
        return []
