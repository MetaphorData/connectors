import asyncio
from abc import ABC, abstractmethod
from typing import Collection, Optional

from metaphor.common.base_config import BaseConfig
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.models.crawler_run_metadata import Platform


class BaseExtractor(ABC):
    """Base class for metadata extractors"""

    _platform: Optional[Platform]
    _description: str

    @staticmethod
    @abstractmethod
    def from_config_file(config_file: str) -> "BaseExtractor":
        """Returns the corresponding extractor class"""

    @abstractmethod
    async def extract(self) -> Collection[ENTITY_TYPES]:
        """Extract metadata and build messages, should be overridden"""

    def __init__(self, config: BaseConfig) -> None:
        self._output = config.output

    def run_async(self) -> Collection[ENTITY_TYPES]:
        return asyncio.run(self.extract())
