from typing import Collection, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    Dashboard,
    Dataset,
    MetadataChangeEvent,
    VirtualView,
)


@dataclass(config=ConnectorConfig)
class DummyRunConfig(BaseConfig):
    dummy_attr: int


class DummyExtractor(BaseExtractor):
    def platform(self) -> Optional[Platform]:
        return None

    def description(self) -> str:
        return "dummy crawler"

    @staticmethod
    def from_config_file(config_file: str) -> "DummyExtractor":
        return DummyExtractor(DummyRunConfig(dummy_attr=1, output=OutputConfig()), [])

    def __init__(self, config: DummyRunConfig, dummy_entities):
        super().__init__(config, "description", None)
        self._dummy_entities = dummy_entities

    async def extract(self) -> Collection[ENTITY_TYPES]:
        return self._dummy_entities


def test_dummy_extractor():
    entities = [Dashboard(), Dataset(), VirtualView()]
    config = DummyRunConfig(dummy_attr=0, output=OutputConfig())
    events = DummyExtractor(config, entities).run()

    assert events == [
        MetadataChangeEvent(dashboard=Dashboard()),
        MetadataChangeEvent(dataset=Dataset()),
        MetadataChangeEvent(virtual_view=VirtualView()),
    ]
