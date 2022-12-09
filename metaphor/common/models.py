from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.entity_id import normalize_full_dataset_name, to_dataset_entity_id
from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID


@dataclass
class DeserializableDatasetLogicalID:
    name: str
    platform: str
    account: Optional[str] = None

    def to_logical_id(self) -> DatasetLogicalID:
        return DatasetLogicalID(
            name=normalize_full_dataset_name(self.name),
            platform=DataPlatform[self.platform],
            account=self.account,
        )

    def to_entity_id(self):
        return to_dataset_entity_id(
            normalize_full_dataset_name(self.name),
            DataPlatform[self.platform],
            self.account,
        )
