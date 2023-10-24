from pydantic import Field
from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class SamplingConfig:
    """Config for profile connector"""

    # Sampling percentage, i.e. 1 means 1% of rows will be sampled. Value must be between 1 and 100
    percentage: int = Field(default=100, ge=1, le=100)

    # Sampling only affect table large than threshold
    threshold: int = 100000
