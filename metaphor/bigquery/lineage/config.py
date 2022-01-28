from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig


@deserialize
@dataclass
class BigQueryLineageRunConfig(BigQueryRunConfig):
    lookback_days: int = 30

    batch_size: int = 1000
