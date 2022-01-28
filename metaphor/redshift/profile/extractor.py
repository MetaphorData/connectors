from typing import List

from metaphor.models.metadata_change_event import DataPlatform, MetadataChangeEvent

from metaphor.common.logger import get_logger
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.postgresql.profile.extractor import PostgreSQLProfileExtractor
from metaphor.redshift.profile.config import RedshiftProfileRunConfig

logger = get_logger(__name__)


class RedshiftProfileExtractor(PostgreSQLProfileExtractor):
    """Redshift data profile extractor"""

    @staticmethod
    def config_class():
        return RedshiftProfileRunConfig

    def __init__(self):
        super().__init__()
        self._platform = DataPlatform.REDSHIFT

    async def extract(
        self, config: RedshiftProfileRunConfig
    ) -> List[MetadataChangeEvent]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching data profile from redshift host {config.host}")
        return await self._extract(config)
