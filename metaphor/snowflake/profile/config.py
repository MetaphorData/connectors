from dataclasses import dataclass, field

from serde import deserialize

from metaphor.common.sampling import SamplingConfig
from metaphor.snowflake.config import SnowflakeRunConfig


@deserialize
@dataclass
class SnowflakeProfileRunConfig(SnowflakeRunConfig):

    # Include views in profiling
    include_views: bool = False

    sampling: SamplingConfig = field(default_factory=lambda: SamplingConfig())
