from dataclasses import field as dataclass_field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.bigquery.config import BigQueryRunConfig


@dataclass
class BigQueryUsageRunConfig(BigQueryRunConfig):
    # whether to write to dataset usage history or dataset usage aspect
    use_history: bool = True

    # the number of days' logs to fetch if use_history = False, otherwise, it's locked to 1 day
    lookback_days: int = 30

    batch_size: int = 1000

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())
