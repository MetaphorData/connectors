from typing import List, Optional

from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID
from pydantic.dataclasses import dataclass

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.extractor import BaseConfig


@dataclass
class DeserializableDatasetLogicalID:
    name: str
    platform: str
    account: Optional[str] = None

    def to_logical_id(self) -> DatasetLogicalID:
        return DatasetLogicalID(
            name=self.name, platform=DataPlatform[self.platform], account=self.account
        )

    def to_entity_id(self):
        return to_dataset_entity_id(
            self.name, DataPlatform[self.platform], self.account
        )


@dataclass
class Lineage:
    dataset: DeserializableDatasetLogicalID
    upstreams: List[DeserializableDatasetLogicalID]


@dataclass
class ManualLienageConfig(BaseConfig):
    lineages: List[Lineage]
