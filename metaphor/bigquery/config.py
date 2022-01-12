from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import List, Optional

from serde import deserialize

from metaphor.common.extractor import RunConfig


@deserialize
@dataclass
class BigQueryRunConfig(RunConfig):
    # Path to service account's JSON key file
    key_path: str

    # Project ID to use. Use the service account's default project if not set
    project_id: Optional[str] = None

    # Filters for dataset names (any match will be included)
    dataset_filters: List[str] = dataclass_field(default_factory=lambda: [r".*"])
