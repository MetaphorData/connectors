from typing import List

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.models import DeserializableDatasetLogicalID


@dataclass
class Lineage:
    dataset: DeserializableDatasetLogicalID
    upstreams: List[DeserializableDatasetLogicalID]


@dataclass
class CustomLineageConfig(BaseConfig):
    lineages: List[Lineage]
