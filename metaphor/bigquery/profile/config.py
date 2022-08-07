from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.common.column_statistics import ColumnStatistics
from metaphor.common.sampling import SamplingConfig


@dataclass
class BigQueryProfileRunConfig(BigQueryRunConfig):

    # Compute specific types of statistics for each column
    column_statistics: ColumnStatistics = field(
        default_factory=lambda: ColumnStatistics()
    )

    sampling: SamplingConfig = field(default_factory=lambda: SamplingConfig())
