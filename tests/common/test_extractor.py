from datetime import datetime, timezone
from typing import Collection, Optional, Type

from freezegun import freeze_time
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    Dashboard,
    Dataset,
    EventHeader,
    MetadataChangeEvent,
    VirtualView,
)


@dataclass
class DummyRunConfig(BaseConfig):
    dummy_attr: int


class DummyExtractor(BaseExtractor):
    def platform(self) -> Optional[Platform]:
        return None

    def description(self) -> str:
        return "dummy crawler"

    @staticmethod
    def from_config_file(config_file: str) -> Type[BaseConfig]:
        return DummyRunConfig.from_config_file(config_file)

    def __init__(self, config: DummyRunConfig, dummy_entities):
        super().__init__(config, "description", None)
        self._dummy_entities = dummy_entities

    async def extract(self) -> Collection[ENTITY_TYPES]:
        return self._dummy_entities


@freeze_time("2000-01-01")
def test_dummy_extractor():
    entities = [Dashboard(), Dataset(), VirtualView()]
    config = DummyRunConfig(dummy_attr=0, output=OutputConfig())
    events = DummyExtractor(config, entities).run()

    assert events == [
        MetadataChangeEvent(
            dashboard=Dashboard(),
            event_header=EventHeader(
                app_name="tests.common.test_extractor.DummyExtractor",
                server="",
                time=datetime(2000, 1, 1, 0, 0, tzinfo=timezone.utc),
            ),
        ),
        MetadataChangeEvent(
            dataset=Dataset(),
            event_header=EventHeader(
                app_name="tests.common.test_extractor.DummyExtractor",
                server="",
                time=datetime(2000, 1, 1, 0, 0, tzinfo=timezone.utc),
            ),
        ),
        MetadataChangeEvent(
            virtual_view=VirtualView(),
            event_header=EventHeader(
                app_name="tests.common.test_extractor.DummyExtractor",
                server="",
                time=datetime(2000, 1, 1, 0, 0, tzinfo=timezone.utc),
            ),
        ),
    ]
