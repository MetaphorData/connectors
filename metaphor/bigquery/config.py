from dataclasses import field as dataclass_field
from typing import Optional

from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.common.base_config import BaseConfig
from metaphor.common.filter import DatasetFilter


@deserialize
@dataclass
class BigQueryRunConfig(BaseConfig):
    # Path to service account's JSON key file
    key_path: str

    # Project ID to use. Use the service account's default project if not set
    project_id: Optional[str] = None

    # Include or exclude specific databases/schemas/tables
    filter: Optional[DatasetFilter] = dataclass_field(
        default_factory=lambda: DatasetFilter()
    )
