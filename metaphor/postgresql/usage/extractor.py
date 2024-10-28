from typing import Collection
from warnings import warn

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.postgresql.extractor import PostgreSQLExtractor
from metaphor.postgresql.usage.config import PostgreSQLUsageRunConfig


class PostgreSQLUsageExtractor(PostgreSQLExtractor):
    """PostgreSQL usage metadata extractor"""

    _description = "PostgreSQL usage statistics crawler"

    @staticmethod
    def from_config_file(config_file: str) -> "PostgreSQLUsageExtractor":
        return PostgreSQLUsageExtractor(
            PostgreSQLUsageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: PostgreSQLUsageRunConfig):
        super().__init__(config)
        warn(
            "PostgreSQL usage crawler is deprecated, and is marked for removal in 0.15.0",
            DeprecationWarning,
            stacklevel=2,
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        # Deprecated connector, do nothing!
        return []
