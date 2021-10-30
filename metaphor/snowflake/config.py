from dataclasses import dataclass, field
from typing import Optional

from serde import deserialize

from metaphor.snowflake.auth import SnowflakeAuthConfig
from metaphor.snowflake.filter import SnowflakeFilter
from metaphor.snowflake.utils import DEFAULT_THREAD_POOL_SIZE


@deserialize
@dataclass
class SnowflakeRunConfig(SnowflakeFilter, SnowflakeAuthConfig):

    # Include or exclude specific databases/schemas/tables
    filter: Optional[SnowflakeFilter] = field(default_factory=lambda: SnowflakeFilter())

    # Max number of concurrent queries to database
    max_concurrency: Optional[int] = DEFAULT_THREAD_POOL_SIZE
