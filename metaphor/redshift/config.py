from dataclasses import field
from typing import List

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.tag_matcher import TagMatcher
from metaphor.postgresql.config import BasePostgreSQLRunConfig


@dataclass(config=ConnectorConfig)
class RedshiftRunConfig(BasePostgreSQLRunConfig):
    port: int = 5439

    # How tags should be assigned to datasets
    tag_matchers: List[TagMatcher] = field(default_factory=lambda: [])
