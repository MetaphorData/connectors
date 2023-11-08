from dataclasses import field as dataclass_field
from typing import Dict, List

from pydantic.dataclasses import dataclass

from metaphor.common.base_extractor import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.utils import is_email
from metaphor.models.metadata_change_event import (
    DataPlatform,
    QueryAttribution,
    QueryAttributions,
)


@dataclass(config=ConnectorConfig)
class PlatformQueryAttributions:
    platform: DataPlatform = DataPlatform.UNKNOWN
    queries: Dict[str, str] = dataclass_field(default_factory=lambda: dict())

    def to_mce_query_attributions(self) -> QueryAttributions:
        return QueryAttributions(
            platform=self.platform,
            queries=[
                QueryAttribution(query_id=k, user_email=v)
                for k, v in self.queries.items()
                if is_email(v)
            ],
        )


@dataclass(config=ConnectorConfig)
class CustomQueryAttributionsConfig(BaseConfig):
    attributions: List[PlatformQueryAttributions] = dataclass_field(
        default_factory=lambda: list()
    )
