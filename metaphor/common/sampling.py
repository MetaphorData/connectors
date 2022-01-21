from dataclasses import dataclass

from serde import deserialize


@deserialize
@dataclass
class SamplingConfig:
    """Config for profile connector"""

    # Sampling percentage, i.e. 1 means 1% of rows will be sampled. Value must be between 1 and 100
    percentage: int = 100

    # Sampling only affect table large than threshold
    threshold: int = 100000
