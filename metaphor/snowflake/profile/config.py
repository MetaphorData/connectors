from dataclasses import dataclass
from typing import Optional

from serde import deserialize

from metaphor.snowflake.config import SnowflakeRunConfig


@deserialize
@dataclass
class SnowflakeProfileRunConfig(SnowflakeRunConfig):

    # Include views in profiling
    include_views: bool = False

    # Sampling percentage, i.e. 1 means 1% of rows will be sampled. Value must be between 0 and 100
    sampling_percentage: Optional[float] = None
