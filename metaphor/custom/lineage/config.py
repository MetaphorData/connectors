from typing import List

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.models import DeserializableDatasetLogicalID


@dataclass(config=ConnectorConfig)
class Lineage:
    dataset: DeserializableDatasetLogicalID
    upstreams: List[DeserializableDatasetLogicalID]


@dataclass(config=ConnectorConfig)
class CustomLineageConfig(BaseConfig):
    lineages: List[Lineage]
