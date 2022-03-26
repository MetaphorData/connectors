from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.sampling import SamplingConfig
from metaphor.snowflake.config import SnowflakeRunConfig


@dataclass
class ColumnStatistics:
    # Compute null and non-null counts
    null_count: bool = True

    # Compute unique value counts
    unique_count: bool = False

    # Compute min value
    min_value: bool = True

    # Compute max value
    max_value: bool = True

    # Compute average value
    avg_value: bool = False

    # Compute standard deviation
    std_dev: bool = False


@dataclass
class SnowflakeProfileRunConfig(SnowflakeRunConfig):

    # Compute specific types of statistics for each column
    column_statistics: ColumnStatistics = field(
        default_factory=lambda: ColumnStatistics()
    )

    # Include views in profiling
    include_views: bool = False

    sampling: SamplingConfig = field(default_factory=lambda: SamplingConfig())
