import asyncio
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Type

from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, Status
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
        start_time = datetime.now()
        logger.info(f"Starting extractor {self.__class__.__name__} at {start_time}")

        run_status = Status.SUCCESS

        events: List[MetadataChangeEvent] = []
        try:
            events = asyncio.run(self.extract(config))
        except Exception as ex:
            run_status = Status.FAILURE
            logger.error(ex)
            traceback.format_exc()

        end_time = datetime.now()
        entity_count = len(events)
        logger.info(
            f"Extractor ended with {run_status} at {end_time}, fetched {entity_count} entities, took {format((end_time - start_time).total_seconds(), '.1f')}s"
        )

        crawler_name = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        run_metadata = CrawlerRunMetadata(
            crawler_name=crawler_name,
            start_time=start_time,
            end_time=end_time,
            status=run_status,
            entity_count=float(entity_count),
        )

        if config.output.api is not None:
            ApiSink(config.output.api).sink(events)

        file_sink = None
        if config.output.file is not None:
            file_sink = FileSink(config.output.file)
            file_sink.sink(events)

        if file_sink is not None:
            file_sink.sink_metadata(run_metadata)
            file_sink.sink_logs()

        return events

    @abstractmethod
    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        """Extract metadata and build messages, should be overridden"""
