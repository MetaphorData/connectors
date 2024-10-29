from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel


class TableLineage(BaseModel):
    upstream_tables: List[str] = []


class Column(BaseModel):
    column_name: str
    table_name: str


class ColumnLineage(BaseModel):
    upstream_columns: Dict[str, List[Column]] = {}


class Tag(BaseModel):
    key: str
    value: str


class CatalogInfo(BaseModel):
    catalog_name: str
    owner: str
    comment: Optional[str] = None
    tags: List[Tag]


class SchemaInfo(BaseModel):
    catalog_name: str
    schema_name: str
    owner: str
    comment: Optional[str] = None
    tags: List[Tag]


class ColumnInfo(BaseModel):
    column_name: str
    data_type: str
    data_precision: Optional[int]
    is_nullable: bool
    comment: Optional[str] = None
    tags: List[Tag]


class TableInfo(BaseModel):
    catalog_name: str
    schema_name: str
    table_name: str
    type: str
    owner: str
    comment: Optional[str] = None
    created_at: datetime
    created_by: str
    updated_at: datetime
    updated_by: str
    view_definition: Optional[str] = None
    storage_location: Optional[str] = None
    data_source_format: str
    tags: List[Tag] = []
    columns: List[ColumnInfo] = []


class VolumeInfo(BaseModel):
    catalog_name: str
    schema_name: str
    volume_name: str
    volume_type: Literal["MANAGED", "EXTERNAL"]
    full_name: str
    owner: str
    comment: Optional[str] = None
    created_at: datetime
    created_by: str
    updated_at: datetime
    updated_by: str
    storage_location: str
    tags: List[Tag]


class VolumeFileInfo(BaseModel):
    last_updated: datetime
    name: str
    path: str
    size: float
