import asyncio
import traceback
from abc import ABC, abstractmethod
from typing import Collection, Iterator, Optional

from metaphor.common.base_config import BaseConfig
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.models.crawler_run_metadata import Platform, RunStatus
from metaphor.models.metadata_change_event import QueryLog


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
        self.error_message: Optional[str] = None
        self.stacktrace: Optional[str] = None

    @property
    def status(self) -> RunStatus:
        if self.error_message or self.stacktrace:
            return RunStatus.FAILURE
        return RunStatus.SUCCESS

    def run_async(self) -> Collection[ENTITY_TYPES]:
        return asyncio.run(self.extract())

    def collect_query_logs(self) -> Iterator[QueryLog]:
        """
        Collects only the query logs. By default collects nothing,
        crawler class needs to implement this method if it wishes
        to separate the query logs from the rest of the MCEs.
        """
        yield from []

    def extend_errors(self, e: Exception) -> None:
        error_message = str(e)
        stacktrace = traceback.format_exc()
        if not self.error_message:
            self.error_message = error_message
        else:
            self.error_message += f"\n{error_message}"
        if not self.stacktrace:
            self.stacktrace = stacktrace
        else:
            self.stacktrace += f"\n{stacktrace}"
