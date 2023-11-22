import datetime
from typing import Any, Generic, List, Optional, TypeVar

from pydantic import Field

from metaphor.common.models import V1CompatBaseModel

DataT = TypeVar("DataT")


class GenericData(V1CompatBaseModel, Generic[DataT]):
    next_cursor: Optional[str] = None
    items: List[DataT]


class GenericListResponse(V1CompatBaseModel, Generic[DataT]):
    code: str
    data: Optional[GenericData[DataT]] = None


class GenericResponse(V1CompatBaseModel, Generic[DataT]):
    code: str
    data: Optional[DataT] = None


class MetadataSchemaPayload(V1CompatBaseModel):
    id: str
    name_in_source: Optional[str] = None
    name_in_destination: str


class MetadataTablePayload(V1CompatBaseModel):
    id: str
    parent_id: str
    name_in_source: str
    name_in_destination: str


class MetadataColumnPayload(V1CompatBaseModel):
    id: str
    parent_id: str
    name_in_source: str
    name_in_destination: str
    type_in_source: Optional[str] = None
    type_in_destination: Optional[str] = None
    is_primary_key: bool
    is_foreign_key: bool


class GroupPayload(V1CompatBaseModel):
    id: str
    name: str
    created_at: datetime.datetime


class DestinationPayload(V1CompatBaseModel):
    id: str
    group_id: str
    service: str
    region: str
    time_zone_offset: str
    setup_status: str
    config: Any = None


class ConnectorStatus(V1CompatBaseModel):
    setup_state: str
    update_state: str
    sync_state: str
    is_historical_sync: bool


class ConnectorPayload(V1CompatBaseModel):
    id: str
    group_id: str
    service: str
    paused: bool
    schema_: str = Field(alias="schema")
    succeeded_at: Optional[datetime.datetime] = None
    sync_frequency: int
    connected_by: Optional[str] = None
    service_version: int
    created_at: datetime.datetime
    failed_at: Optional[datetime.datetime] = None
    status: ConnectorStatus
    config: Any = None

    def __hash__(self):
        return hash(self.id)


class SourceMetadataPayload(V1CompatBaseModel):
    id: str
    name: str
    type: str
    description: Optional[str] = None
    icon_url: Optional[str] = None


class UserPayload(V1CompatBaseModel):
    id: str
    email: str
