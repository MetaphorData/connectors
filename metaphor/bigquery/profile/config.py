from dataclasses import dataclass
from typing import Optional

from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.common.extractor import SamplingConfig


@deserialize
@dataclass
class BigQueryProfileRunConfig(BigQueryRunConfig):

    sampling: Optional[SamplingConfig] = None
