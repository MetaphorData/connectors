from dataclasses import field as dataclass_field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.redshift.config import RedshiftRunConfig


@dataclass
class RedshiftQueryRunConfig(RedshiftRunConfig):
    # Number of days of query logs to process if use_history = False, otherwise, it's locked to 1 day
    lookback_days: int = 30

    # Maximum number of recent queries to capture for each table
    max_queries_per_table: int = 100

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())
