from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import List, Optional, Set

from serde import deserialize

from metaphor.common.extractor import RunConfig


@deserialize
@dataclass
class BigQueryUsageRunConfig(RunConfig):
    # Path to service account's JSON key file
    key_path: str

    # Project ID to use. Use the service account's default project if not set
    project_id: Optional[str] = None

    lookback_days: int = 30

    batch_size: int = 1000

    dataset_filters: List[str] = dataclass_field(default_factory=lambda: [r".*"])

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())
