import datetime
from typing import Any, Generic, List, Optional, TypeVar

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

DataT = TypeVar("DataT")


class GenericData(GenericModel, Generic[DataT]):
    next_cursor: Optional[str]
    items: List[DataT]


class GenericListResponse(GenericModel, Generic[DataT]):
    code: str
    data: Optional[GenericData[DataT]]


class GenericResponse(GenericModel, Generic[DataT]):
    code: str
    data: Optional[DataT]


class MetadataSchemaPayload(BaseModel):
    id: str
    name_in_source: Optional[str]
    name_in_destination: str


class MetadataTablePayload(BaseModel):
    id: str
    parent_id: str
    name_in_source: str
    name_in_destination: str


class MetadataColumnPayload(BaseModel):
    id: str
    parent_id: str
    name_in_source: str
    name_in_destination: str
    type_in_source: Optional[str]
    type_in_destination: Optional[str]
    is_primary_key: bool
    is_foreign_key: bool


class GroupPayload(BaseModel):
    id: str
    name: str
    created_at: datetime.datetime


class DestinationPayload(BaseModel):
    id: str
    group_id: str
    service: str
    region: str
    time_zone_offset: str
    setup_status: str
    config: Any


class ConnectorStatus(BaseModel):
    setup_state: str
    update_state: str
    sync_state: str
    is_historical_sync: str


class ConnectorPayload(BaseModel):
    id: str
    group_id: str
    service: str
    paused: bool
    schema_: str = Field(alias="schema")
    succeeded_at: Optional[datetime.datetime]
    sync_frequency: int
    connected_by: Optional[str]
    service_version: int
    created_at: datetime.datetime
    failed_at: Optional[datetime.datetime]
    status: ConnectorStatus
    config: Any

    def __hash__(self):
        return hash(self.id)


class SourceMetadataPayload(BaseModel):
    id: str
    name: str
    type: str
    description: Optional[str]
    icon_url: Optional[str]


class UserPayload(BaseModel):
    id: str
    email: str
