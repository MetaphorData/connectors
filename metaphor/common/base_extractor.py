import asyncio
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Collection, List, Optional, Type

from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, Platform, Status
from metaphor.models.metadata_change_event import MetadataChangeEvent

from .api_sink import ApiSink
from .base_config import BaseConfig
from .event_util import ENTITY_TYPES, EventUtil
from .file_sink import FileSink

logger = get_logger()


class BaseExtractor(ABC):
    """Base class for metadata extractors"""

    @staticmethod
    @abstractmethod
    def from_config_file(config_file: str) -> Type[BaseConfig]:
        """Returns the corresponding config class"""

    @abstractmethod
    async def extract(self) -> Collection[ENTITY_TYPES]:
        """Extract metadata and build messages, should be overridden"""

    def __init__(
        self, config: BaseConfig, description: str, platform: Optional[Platform]
    ) -> None:
        self._output = config.output
        self._description = description
        self._platform = platform

    def run(self) -> List[MetadataChangeEvent]:
        """Callable function to extract metadata and send/post messages"""
        start_time = datetime.now()
        logger.info(f"Starting extractor {self.__class__.__name__} at {start_time}")

        run_status = Status.SUCCESS
        error_message = None
        stacktrace = None

        entities: Collection[ENTITY_TYPES] = []
        try:
            entities = asyncio.run(self.extract())
        except Exception as ex:
            run_status = Status.FAILURE
            error_message = str(ex)
            stacktrace = traceback.format_exc()
            logger.exception(ex)

        end_time = datetime.now()
        entity_count = len(entities)
        logger.info(
            f"Extractor ended with {run_status} at {end_time}, fetched {entity_count} entities, took {format((end_time - start_time).total_seconds(), '.1f')}s"
        )

        crawler_name = EventUtil.class_fqcn(self.__class__)
        event_util = EventUtil(crawler_name)
        events = [event_util.build_event(entity) for entity in entities]

        run_metadata = CrawlerRunMetadata(
            crawler_name=crawler_name,
            platform=self._platform,
            description=self._description,
            start_time=start_time,
            end_time=end_time,
            status=run_status,
            entity_count=float(entity_count),
            error_message=error_message,
            stack_trace=stacktrace,
        )

        if self._output.api is not None:
            ApiSink(self._output.api).sink(events)

        file_sink = None
        if self._output.file is not None:
            file_sink = FileSink(self._output.file)
            file_sink.sink(events)

        if file_sink is not None:
            file_sink.sink_metadata(run_metadata)
            file_sink.sink_logs()

        return events
