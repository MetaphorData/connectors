from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Set

from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig


@deserialize
@dataclass
class BigQueryUsageRunConfig(BigQueryRunConfig):
    lookback_days: int = 30

    batch_size: int = 1000

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())
