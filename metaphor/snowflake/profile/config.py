from dataclasses import dataclass

from serde import deserialize

from metaphor.snowflake.config import SnowflakeRunConfig


@deserialize
@dataclass
class SnowflakeProfileRunConfig(SnowflakeRunConfig):

    # Include views in profiling
    include_views: bool = False
