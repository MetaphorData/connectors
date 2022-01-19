from dataclasses import dataclass

from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig


@deserialize
@dataclass
class BigQueryProfileRunConfig(BigQueryRunConfig):
    pass
