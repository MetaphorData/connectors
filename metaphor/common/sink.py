import json
import logging
from abc import ABC, abstractmethod
from typing import Generator, List

from metaphor.models.metadata_change_event import MetadataChangeEvent

from .event_util import EventUtil

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Sink(ABC):
    """Base class for metadata sinks"""

    def sink(self, events: List[MetadataChangeEvent]) -> bool:
        """Sink MCE messages to the destination"""
        records = [EventUtil.trim_event(e) for e in events]
        valid_records = [r for r in records if EventUtil.validate_message(r)]
        logger.debug(f"Records: {json.dumps(valid_records)}")
        if len(valid_records) == 0:
            return False

        return self._sink(valid_records)

    @staticmethod
    def _chunks(records: List, n: int) -> Generator[List, None, None]:
        """Yield successive n-sized chunks from list."""
        for i in range(0, len(records), n):
            yield records[i : i + n]

    @abstractmethod
    def _sink(self, messages: List[dict]) -> bool:
        """Sink metadata records to the destination, should be overridden"""
