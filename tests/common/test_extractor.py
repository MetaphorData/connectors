from typing import List, Type

import pytest
from metaphor.models.metadata_change_event import MetadataChangeEvent
from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.extractor import BaseExtractor


@deserialize
@dataclass
class DummyRunConfig(BaseConfig):
    dummy_attr: int


class DummyExtractor(BaseExtractor):
    def __init__(self, dummy_events):
        self._dummy_events = dummy_events

    @staticmethod
    def config_class() -> Type[BaseConfig]:
        return DummyRunConfig

    async def extract(self, config: BaseConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, DummyExtractor.config_class())
        return self._dummy_events


@pytest.mark.asyncio
def test_dummy_extractor():
    events = [MetadataChangeEvent()]
    config = DummyRunConfig(dummy_attr=0, output=OutputConfig())
    assert events == DummyExtractor(events).run(config)
