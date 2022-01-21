from dataclasses import dataclass
from typing import Optional

from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.common.sampling import SamplingConfig


@deserialize
@dataclass
class BigQueryProfileRunConfig(BigQueryRunConfig):

    sampling: Optional[SamplingConfig] = None
