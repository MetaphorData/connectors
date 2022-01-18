from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import List, Set

from serde import deserialize

from metaphor.redshift.config import RedshiftRunConfig


@deserialize
@dataclass
class RedshiftUsageRunConfig(RedshiftRunConfig):
    lookback_days: int = 30

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())

    # Filters for dataset names (any match will be included)
    dataset_filters: List[str] = dataclass_field(default_factory=lambda: [r".*"])
