from typing import List

from metaphor.models.metadata_change_event import MetadataChangeEvent

from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.redshift.config import RedshiftSQLRunConfig


class RedshiftSQLExtractor(PostgreSQLExtractor):
    """Redshift metadata extractor"""

    async def extract(self, config: RedshiftSQLRunConfig) -> List[MetadataChangeEvent]:
        return self._extract(config, True)
