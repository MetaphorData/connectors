from dataclasses import field as dataclass_field
from typing import List

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.entity_id import to_person_entity_id
from metaphor.common.models import DeserializableDatasetLogicalID
from metaphor.models.metadata_change_event import (
    AssetDescription,
    ColumnDescriptionAssignment,
)


@dataclass(config=ConnectorConfig)
class Ownership:
    # Type of ownership to assign
    type: str

    # Owner's email address
    email: str


@dataclass(config=ConnectorConfig)
class ColumnTags:
    # Name of the column
    column: str

    # List of associated tags
    tags: List[str]


@dataclass(config=ConnectorConfig)
class Description:
    # The description to assign
    description: str

    # The author's email address
    email: str

    def to_asset_description(self) -> AssetDescription:
        return AssetDescription(
            author=str(to_person_entity_id(self.email)),
            description=self.description,
        )


@dataclass(config=ConnectorConfig)
class ColumnDescriptions:
    # The column's descriptions
    descriptions: List[Description]

    # Name of the column
    column_name: str

    def to_column_asset_description(self) -> ColumnDescriptionAssignment:
        return ColumnDescriptionAssignment(
            asset_descriptions=[d.to_asset_description() for d in self.descriptions],
            column_name=self.column_name,
        )


@dataclass(config=ConnectorConfig)
class DatasetGovernance:
    id: DeserializableDatasetLogicalID

    ownerships: List[Ownership] = dataclass_field(default_factory=lambda: [])

    tags: List[str] = dataclass_field(default_factory=lambda: [])

    column_tags: List[ColumnTags] = dataclass_field(default_factory=lambda: [])

    descriptions: List[Description] = dataclass_field(default_factory=lambda: [])

    column_descriptions: List[ColumnDescriptions] = dataclass_field(
        default_factory=lambda: []
    )


@dataclass(config=ConnectorConfig)
class CustomGovernanceConfig(BaseConfig):
    datasets: List[DatasetGovernance]
