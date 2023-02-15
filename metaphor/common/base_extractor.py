import asyncio
from abc import ABC, abstractmethod
from typing import Collection, List, Optional

from metaphor.common.base_config import BaseConfig
from metaphor.common.event_util import ENTITY_TYPES, EventUtil
from metaphor.common.runner import run_connector
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import MetadataChangeEvent


class BaseExtractor(ABC):
    """Base class for metadata extractors"""

    @staticmethod
    @abstractmethod
    def from_config_file(config_file: str) -> "BaseExtractor":
        """Returns the corresponding extractor class"""

    @abstractmethod
    async def extract(self) -> Collection[ENTITY_TYPES]:
        """Extract metadata and build messages, should be overridden"""

    def __init__(
        self, config: BaseConfig, description: str, platform: Optional[Platform]
    ) -> None:
        self._output = config.output
        self._description = description
        self._platform = platform

    def run_async(self) -> Collection[ENTITY_TYPES]:
        return asyncio.run(self.extract())

    def run(self) -> List[MetadataChangeEvent]:
        """Callable function to extract metadata and send/post messages"""

        return run_connector(
            connector_func=self.run_async,
            name=EventUtil.class_fqcn(self.__class__),
            platform=self._platform,
            description=self._description,
            file_sink_config=self._output.file,
        )
