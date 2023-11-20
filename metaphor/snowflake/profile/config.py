from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.column_statistics import ColumnStatistics
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.sampling import SamplingConfig
from metaphor.snowflake.config import SnowflakeBaseConfig


@dataclass(config=ConnectorConfig)
class SnowflakeProfileRunConfig(SnowflakeBaseConfig):
    # Compute specific types of statistics for each column
    column_statistics: ColumnStatistics = field(
        default_factory=lambda: ColumnStatistics()
    )

    # Include views in profiling
    include_views: bool = False

    sampling: SamplingConfig = field(default_factory=lambda: SamplingConfig())
