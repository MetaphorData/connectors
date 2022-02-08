from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.bigquery.config import BigQueryRunConfig


@deserialize
@dataclass
class BigQueryLineageRunConfig(BigQueryRunConfig):
    # Whether to enable parsing view query to find upstream of the view, default True
    enable_view_lineage: bool = True

    # Whether to enable parsing audit log to find table lineage information, default True
    enable_lineage_from_log: bool = True

    lookback_days: int = 30

    batch_size: int = 1000
