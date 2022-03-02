from dataclasses import field as dataclass_field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.redshift.config import RedshiftRunConfig


@dataclass
class RedshiftUsageRunConfig(RedshiftRunConfig):
    # whether to write to dataset usage history or dataset usage aspect
    use_history: bool = True

    # Number of days of query logs to process if use_history = False, otherwise, it's locked to 1 day
    lookback_days: int = 30

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())
