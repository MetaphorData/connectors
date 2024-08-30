from dataclasses import field as dataclass_field

from pydantic.dataclasses import dataclass

from metaphor.common.column_statistics import ColumnStatistics
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.sampling import SamplingConfig
from metaphor.postgresql.config import BasePostgreSQLRunConfig


@dataclass(config=ConnectorConfig)
class PostgreSQLProfileRunConfig(BasePostgreSQLRunConfig):
    # Compute specific types of statistics for each column
    column_statistics: ColumnStatistics = dataclass_field(
        default_factory=lambda: ColumnStatistics()
    )

    max_concurrency = 10

    include_views: bool = False

    sampling: SamplingConfig = dataclass_field(default_factory=lambda: SamplingConfig())
