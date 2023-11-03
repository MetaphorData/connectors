from dataclasses import field as dataclass_field
from typing import List, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.entity_id import to_person_entity_id
from metaphor.common.models import DeserializableDatasetLogicalID
from metaphor.models.metadata_change_event import (
    AssetDescription,
    ColumnDescriptionAssignment,
    ColumnTagAssignment,
    DescriptionAssignment,
)
from metaphor.models.metadata_change_event import (
    Ownership as OwnershipAssignmentOwnership,
)
from metaphor.models.metadata_change_event import OwnershipAssignment, TagAssignment


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

    def to_ownership_assignment(self) -> Optional[OwnershipAssignment]:
        if not self.ownerships:
            return None

        ownerships = [
            OwnershipAssignmentOwnership(
                contact_designation_name=o.type,
                person=str(to_person_entity_id(o.email)),
            )
            for o in self.ownerships
        ]

        return OwnershipAssignment(ownerships=ownerships)

    def to_tag_assignment(self) -> Optional[TagAssignment]:
        if not self.tags and not self.column_tags:
            return None

        tag_assignment = TagAssignment()
        if self.tags:
            tag_assignment.tag_names = self.tags
        if self.column_tags:
            tag_assignment.column_tag_assignments = [
                ColumnTagAssignment(
                    column_name=column_tag.column, tag_names=column_tag.tags
                )
                for column_tag in self.column_tags
            ]

        return tag_assignment

    def to_description_assignment(self) -> Optional[DescriptionAssignment]:
        if not self.descriptions and not self.column_descriptions:
            return None

        description_assignment = DescriptionAssignment()
        if self.descriptions:
            description_assignment.asset_descriptions = [
                d.to_asset_description() for d in self.descriptions
            ]
        if self.column_descriptions:
            description_assignment.column_description_assignments = [
                d.to_column_asset_description() for d in self.column_descriptions
            ]

        return description_assignment


@dataclass(config=ConnectorConfig)
class CustomGovernanceConfig(BaseConfig):
    datasets: List[DatasetGovernance]
