from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.filter import DatasetFilter
from metaphor.snowflake.auth import SnowflakeAuthConfig
from metaphor.snowflake.utils import DEFAULT_THREAD_POOL_SIZE


@dataclass
class SnowflakeRunConfig(SnowflakeAuthConfig):

    # Include or exclude specific databases/schemas/tables
    filter: Optional[DatasetFilter] = field(default_factory=lambda: DatasetFilter())

    # Max number of concurrent queries to database
    max_concurrency: Optional[int] = DEFAULT_THREAD_POOL_SIZE
