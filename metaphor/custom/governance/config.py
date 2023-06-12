from dataclasses import field as dataclass_field
from typing import List

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.models import DeserializableDatasetLogicalID


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


@dataclass(config=ConnectorConfig)
class DatasetGovernance:
    id: DeserializableDatasetLogicalID

    ownerships: List[Ownership] = dataclass_field(default_factory=lambda: [])

    tags: List[str] = dataclass_field(default_factory=lambda: [])

    column_tags: List[ColumnTags] = dataclass_field(default_factory=lambda: [])

    descriptions: List[Description] = dataclass_field(default_factory=lambda: [])


@dataclass(config=ConnectorConfig)
class CustomGovernanceConfig(BaseConfig):
    datasets: List[DatasetGovernance]
