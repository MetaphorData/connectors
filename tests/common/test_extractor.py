from datetime import datetime, timezone
from typing import Collection, Type

from freezegun import freeze_time
from metaphor.models.metadata_change_event import (
    Dashboard,
    Dataset,
    EventHeader,
    MetadataChangeEvent,
    VirtualView,
)
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor


@dataclass
class DummyRunConfig(BaseConfig):
    dummy_attr: int


class DummyExtractor(BaseExtractor):
    def __init__(self, dummy_entities):
        self._dummy_entities = dummy_entities

    @staticmethod
    def config_class() -> Type[BaseConfig]:
        return DummyRunConfig

    async def extract(self, config: BaseConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, DummyExtractor.config_class())
        return self._dummy_entities


@freeze_time("2000-01-01")
def test_dummy_extractor():
    entities = [Dashboard(), Dataset(), VirtualView()]
    config = DummyRunConfig(dummy_attr=0, output=OutputConfig())
    events = DummyExtractor(entities).run(config)

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
