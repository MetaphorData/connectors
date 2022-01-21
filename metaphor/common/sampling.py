from dataclasses import dataclass
from typing import Optional

from serde import deserialize


@deserialize
@dataclass
class SamplingConfig:
    """Config for profile connector"""

    # Sampling percentage, i.e. 1 means 1% of rows will be sampled. Value must be between 0 and 100
    percentage: int

    # Sampling only affect table large than threshold
    threshold: Optional[int] = None
