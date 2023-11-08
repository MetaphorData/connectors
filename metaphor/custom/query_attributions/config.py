from dataclasses import field as dataclass_field
from typing import Dict, List

from pydantic import field_validator
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

    @field_validator("queries")
    def _validate_user_emails(cls, queries_: Dict[str, str]):
        for should_be_email in queries_.values():
            if not is_email(should_be_email):
                raise ValueError(f"Found invalid user email: {should_be_email}")
        return queries_

    def to_mce_query_attributions(self) -> QueryAttributions:
        return QueryAttributions(
            platform=self.platform,
            queries=[
                QueryAttribution(query_id=k, user_email=v)
                for k, v in self.queries.items()
            ],
        )


@dataclass(config=ConnectorConfig)
class CustomQueryAttributionsConfig(BaseConfig):
    attributions: List[PlatformQueryAttributions] = dataclass_field(
        default_factory=lambda: list()
    )
