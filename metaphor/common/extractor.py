import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Type

from metaphor.models.metadata_change_event import MetadataChangeEvent
from serde import deserialize
from serde.json import from_json
from serde.yaml import from_yaml
from smart_open import open

from .api_sink import ApiSink, ApiSinkConfig
from .file_sink import FileSink, FileSinkConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class OutputConfig:
    """Config for where to output the data"""

    api: Optional[ApiSinkConfig] = None
    file: Optional[FileSinkConfig] = None


@deserialize
@dataclass
class RunConfig:
    """Base class for runtime parameters

    All subclasses should add the @dataclass @deserialize decorators
    """

    output: OutputConfig

    @classmethod
    def from_json_file(cls, path: str) -> "RunConfig":
        with open(path, encoding="utf8") as fin:
            return from_json(cls, fin.read())

    @classmethod
    def from_yaml_file(cls, path: str) -> "RunConfig":
        with open(path, encoding="utf8") as fin:
            return from_yaml(cls, fin.read())


class BaseExtractor(ABC):
    """Base class for metadata extractors"""

    @staticmethod
    @abstractmethod
    def config_class() -> Type[RunConfig]:
        """Returns the corresponding config class"""

    def run(self, config: RunConfig) -> List[MetadataChangeEvent]:
        """Callable function to extract metadata and send/post messages"""
        logger.info("Starting extractor {}".format(self.__class__.__name__))

        events: List[MetadataChangeEvent] = asyncio.run(self.extract(config))

        logger.info("Fetched {} entities".format(len(events)))

        if config.output.api is not None:
            ApiSink(config.output.api).sink(events)

        if config.output.file is not None:
            FileSink(config.output.file).sink(events)

        return events

    @abstractmethod
    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        """Extract metadata and build messages, should be overridden"""
