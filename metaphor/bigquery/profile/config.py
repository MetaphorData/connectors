from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.common.sampling import SamplingConfig


@dataclass
class BigQueryProfileRunConfig(BigQueryRunConfig):

    sampling: SamplingConfig = field(default_factory=lambda: SamplingConfig())
