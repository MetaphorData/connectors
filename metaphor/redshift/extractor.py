from typing import List

from metaphor.models.metadata_change_event import MetadataChangeEvent

from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.config import RedshiftRunConfig


class RedshiftExtractor(PostgreSQLExtractor):
    """Redshift metadata extractor"""

    @staticmethod
    def config_class():
        return RedshiftRunConfig

    async def extract(self, config: RedshiftRunConfig) -> List[MetadataChangeEvent]:
        return await self._extract(config, True)
