from typing import List, Type

import pytest
from metaphor.models.metadata_change_event import MetadataChangeEvent
from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.common.extractor import BaseExtractor, OutputConfig, RunConfig


@deserialize
@dataclass
class DummyRunConfig(RunConfig):
    dummy_attr: int


class DummyExtractor(BaseExtractor):
    def __init__(self, dummy_events):
        self._dummy_events = dummy_events

    @staticmethod
    def config_class() -> Type[RunConfig]:
        return DummyRunConfig

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, DummyExtractor.config_class())
        return self._dummy_events


@pytest.mark.asyncio
def test_dummy_extractor():
    events = [MetadataChangeEvent()]
    config = DummyRunConfig(dummy_attr=0, output=OutputConfig())
    assert events == DummyExtractor(events).run(config)
