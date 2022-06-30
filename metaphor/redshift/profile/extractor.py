from typing import Collection, Optional

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.postgresql.profile.extractor import PostgreSQLProfileExtractor
from metaphor.redshift.profile.config import RedshiftProfileRunConfig

logger = get_logger(__name__)


class RedshiftProfileExtractor(PostgreSQLProfileExtractor):
    """Redshift data profile extractor"""

    def platform(self) -> Optional[Platform]:
        return Platform.REDSHIFT

    def description(self) -> str:
        return "Redshift data profile crawler"

    @staticmethod
    def config_class():
        return RedshiftProfileRunConfig

    def __init__(self):
        super().__init__()
        self._platform = DataPlatform.REDSHIFT

    async def extract(
        self, config: RedshiftProfileRunConfig
    ) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, PostgreSQLExtractor.config_class())
        logger.info(f"Fetching data profile from redshift host {config.host}")
        return await self._extract(config)
