from dataclasses import field as dataclass_field
from typing import Set

from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.redshift.config import RedshiftRunConfig


@deserialize
@dataclass
class RedshiftUsageRunConfig(RedshiftRunConfig):
    lookback_days: int = 30

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())
