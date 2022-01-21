from dataclasses import dataclass, field

from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.common.sampling import SamplingConfig


@deserialize
@dataclass
class BigQueryProfileRunConfig(BigQueryRunConfig):

    sampling: SamplingConfig = field(default_factory=lambda: SamplingConfig())
