from dataclasses import dataclass
from typing import Optional

from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig


@deserialize
@dataclass
class SamplingConfig:
    # Sampling percentage, i.e. 1 means 1% of rows will be sampled. Value must be between 0 and 100
    percentage: int

    # Sampling only affect table large than threshold
    threshold: Optional[int] = None


@deserialize
@dataclass
class BigQueryProfileRunConfig(BigQueryRunConfig):

    sampling: Optional[SamplingConfig] = None
