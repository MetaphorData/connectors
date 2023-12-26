from typing import Collection

import pytest
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.models.metadata_change_event import Dashboard, Dataset, VirtualView


@dataclass(config=ConnectorConfig)
class DummyRunConfig(BaseConfig):
    dummy_attr: int


class DummyExtractor(BaseExtractor):
    _platform = None
    _description = "dummy crawler"

    @staticmethod
    def from_config_file(config_file: str) -> "DummyExtractor":
        return DummyExtractor(DummyRunConfig(dummy_attr=1, output=OutputConfig()), [])

    def __init__(self, config: DummyRunConfig, dummy_entities):
        super().__init__(config)
        self._dummy_entities = dummy_entities

    async def extract(self) -> Collection[ENTITY_TYPES]:
        return self._dummy_entities


def test_dummy_extractor():
    entities = [Dashboard(), Dataset(), VirtualView()]
    config = DummyRunConfig(dummy_attr=0, output=OutputConfig())
    parsed_entities = DummyExtractor(config, entities).run_async()

    assert parsed_entities == [
        Dashboard(),
        Dataset(),
        VirtualView(),
    ]


class InvalidExtractor(BaseExtractor):
    @staticmethod
    def from_config_file(config_file: str) -> "InvalidExtractor":
        return InvalidExtractor(BaseConfig(output=OutputConfig()))

    async def extract(self) -> Collection[ENTITY_TYPES]:
        raise AttributeError


def test_invalid_extractor():
    with pytest.raises(AttributeError):
        InvalidExtractor(BaseConfig(output=OutputConfig())).run_async()
