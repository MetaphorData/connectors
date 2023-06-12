from pydantic import validator
from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class SamplingConfig:
    """Config for profile connector"""

    # Sampling percentage, i.e. 1 means 1% of rows will be sampled. Value must be between 1 and 100
    percentage: int = 100

    # Sampling only affect table large than threshold
    threshold: int = 100000

    @validator("percentage")
    def percentage_must_between_1_and_100(cls, v):
        assert 1 <= v <= 100, "Must be between 1 and 100"
        return v
