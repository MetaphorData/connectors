import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Type

from metaphor.models.metadata_change_event import MetadataChangeEvent
from serde import deserialize
from serde.json import from_json
from serde.yaml import from_yaml
from smart_open import open

from metaphor.common.logger import get_logger

from .api_sink import ApiSink, ApiSinkConfig
from .file_sink import FileSink, FileSinkConfig

logger = get_logger(__name__)


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
        start_time = time.time()
        logger.info(f"Starting extractor {self.__class__.__name__} at {datetime.now()}")

        events: List[MetadataChangeEvent] = asyncio.run(self.extract(config))

        logger.info(
            f"Fetched {len(events)} entities, took {format(time.time() - start_time, '.1f')} s"
        )

        if config.output.api is not None:
            ApiSink(config.output.api).sink(events)

        file_sink = None
        if config.output.file is not None:
            file_sink = FileSink(config.output.file)
            file_sink.sink(events)

        logger.info(f"Extractor ended successfully at {datetime.now()}")

        if file_sink is not None:
            file_sink.sink_logs()

        return events

    @abstractmethod
    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        """Extract metadata and build messages, should be overridden"""
