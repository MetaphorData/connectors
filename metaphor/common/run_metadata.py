from dataclasses import dataclass
from datetime import datetime

from serde import deserialize


@deserialize
@dataclass
class CrawlerRunMetadata:
    crawler_name: str
    start_time: datetime
    end_time: datetime
    status: str
    entityCount: int
