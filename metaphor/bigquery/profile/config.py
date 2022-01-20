from dataclasses import dataclass
from typing import Optional

from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig


@deserialize
@dataclass
class BigQueryProfileRunConfig(BigQueryRunConfig):

    # Sampling percentage, i.e. 1 means 1% of rows will be sampled. Value must be between 0 and 100
    sampling_percentage: Optional[int] = None
