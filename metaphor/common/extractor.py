import asyncio
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Type

from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, Status
from metaphor.models.metadata_change_event import MetadataChangeEvent

from metaphor.common.logger import get_logger

from .api_sink import ApiSink
from .base_config import BaseConfig
from .file_sink import FileSink

logger = get_logger(__name__)


class BaseExtractor(ABC):
    """Base class for metadata extractors"""

    @staticmethod
    @abstractmethod
    def config_class() -> Type[BaseConfig]:
        """Returns the corresponding config class"""

    def run(self, config: BaseConfig) -> List[MetadataChangeEvent]:
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
            traceback.print_exc()

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
    async def extract(self, config: BaseConfig) -> List[MetadataChangeEvent]:
        """Extract metadata and build messages, should be overridden"""
