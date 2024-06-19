from typing import Optional, Union

from pydantic import BaseModel, ConfigDict
from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.entity_id import normalize_full_dataset_name, to_dataset_entity_id
from metaphor.common.utils import safe_float
from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    DatasetStatistics,
)


@dataclass(config=ConnectorConfig)
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


def to_dataset_statistics(
    rows: Optional[Union[float, int]] = None,
    size_bytes: Optional[Union[float, int]] = None,
) -> DatasetStatistics:
    """
    Initialize a DatasetStatistics instance
    """
    return DatasetStatistics(
        data_size_bytes=safe_float(size_bytes),
        record_count=safe_float(rows),
    )


class V1CompatBaseModel(BaseModel):
    """
    A Pydantic model that preserves the behavior in Pydantic v1, i.e.
    numbers and `bool`s are parsed to `str`s by default.

    TODO: drop this, just parse numbers to numbers. If it's a string
    then it should be quoted in the source config yaml file.
    """

    model_config = ConfigDict(coerce_numbers_to_str=True)
