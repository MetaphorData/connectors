from pydantic.dataclasses import dataclass

from metaphor.bigquery.config import BigQueryRunConfig


@dataclass
class BigQueryLineageRunConfig(BigQueryRunConfig):
    # Whether to enable parsing view query to find upstream of the view, default True
    enable_view_lineage: bool = True

    # Whether to enable parsing audit log to find table lineage information, default True
    enable_lineage_from_log: bool = True

    lookback_days: int = 7

    # Whether to include self loop in lineage
    include_self_lineage: bool = True

    batch_size: int = 1000
