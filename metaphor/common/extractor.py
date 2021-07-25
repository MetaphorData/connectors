import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from dataclasses_json import dataclass_json
from metaphor.models.metadata_change_event import MetadataChangeEvent
from smart_open import open

from .api_sink import ApiSink, ApiSinkConfig
from .file_sink import FileSink, FileSinkConfig
from .kinesis_sink import KinesisSink, KinesisSinkConfig

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass_json
@dataclass
class RunConfig:
    """Base class for runtime parameters

    All subclasses should add the @dataclass_json & @dataclass decorators
    """

    kinesis: Optional[KinesisSinkConfig]
    api: Optional[ApiSinkConfig]
    file: Optional[FileSinkConfig]

    @classmethod
    def from_json_file(cls, path: str) -> "RunConfig":
        with open(path, encoding="utf8") as fin:
            # Ignored due to https://github.com/lidatong/dataclasses-json/issues/23
            return cls.from_json(fin.read())  # type: ignore


class BaseExtractor(ABC):
    """Base class for metadata extractors"""

    def run(self, config: RunConfig) -> bool:
        """Callable function to extract metadata and send/post messages"""
        logger.info("Starting extractor {}".format(self.__class__.__name__))

        events: List[MetadataChangeEvent] = asyncio.run(self.extract(config))

        logger.info("Fetched {} entities".format(len(events)))

        no_error = True
        if config.kinesis is not None:
            no_error = no_error & KinesisSink(config.kinesis).sink(events)

        if config.api is not None:
            no_error = no_error & ApiSink(config.api).sink(events)

        if config.file is not None:
            no_error = no_error & FileSink(config.file).sink(events)

        logger.info("Execution finished")
        return no_error

    @abstractmethod
    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        """Extract metadata and build messages, should be overridden"""
