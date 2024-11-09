from typing import Collection
from warnings import warn

from metaphor.bigquery.lineage.config import BigQueryLineageRunConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform

logger = get_logger()


class BigQueryLineageExtractor(BaseExtractor):
    """BigQuery lineage metadata extractor"""

    _description = "BigQuery data lineage crawler"
    _platform = Platform.BIGQUERY

    @staticmethod
    def from_config_file(config_file: str) -> "BigQueryLineageExtractor":
        return BigQueryLineageExtractor(
            BigQueryLineageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: BigQueryLineageRunConfig):
        super().__init__(config)
        warn(
            "BigQuery lineage crawler is deprecated, and is marked for removal in 0.15.0",
            DeprecationWarning,
            stacklevel=2,
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        # Deprecated connector, do nothing!
        return []
