from dataclasses import dataclass
from typing import List, Type

import pytest
from metaphor.models.metadata_change_event import MetadataChangeEvent
from serde import deserialize

from metaphor.common.extractor import BaseExtractor, OutputConfig, RunConfig


@deserialize
@dataclass
class DummyRunConfig(RunConfig):
    dummy_events: List[MetadataChangeEvent]


class DummyExtractor(BaseExtractor):
    @staticmethod
    def config_class() -> Type[RunConfig]:
        return DummyRunConfig

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, DummyExtractor.config_class())
        return config.dummy_events


@pytest.mark.asyncio
def test_dummy_extractor():
    events = [MetadataChangeEvent()]
    config = DummyRunConfig(dummy_events=events, output=OutputConfig())
    assert events == DummyExtractor().run(config)
