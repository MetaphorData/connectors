from dataclasses import dataclass
from typing import List, Type

import pytest
from metaphor.models.metadata_change_event import MetadataChangeEvent
from serde import deserialize

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.extractor import BaseExtractor


@deserialize
@dataclass
class DummyRunConfig(BaseConfig):
    dummy_events: List[MetadataChangeEvent]


class DummyExtractor(BaseExtractor):
    @staticmethod
    def config_class() -> Type[BaseConfig]:
        return DummyRunConfig

    async def extract(self, config: BaseConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, DummyExtractor.config_class())
        return config.dummy_events


@pytest.mark.asyncio
def test_dummy_extractor():
    events = [MetadataChangeEvent()]
    config = DummyRunConfig(dummy_events=events, output=OutputConfig())
    assert events == DummyExtractor().run(config)
