from datetime import datetime
from typing import Dict, List, Literal, Optional

from databricks.sql.types import Row
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

    @classmethod
    def from_row_tags(cls, tags: Optional[List[Dict]]) -> List["Tag"]:
        return [
            Tag(key=tag["tag_name"], value=tag["tag_value"])
            for tag in (tags if tags is not None else [])
        ]


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

    @classmethod
    def from_row(cls, row: Row) -> List["ColumnInfo"]:
        return [
            ColumnInfo(
                column_name=column["column_name"],
                data_type=column["data_type"],
                data_precision=column["data_precision"],
                is_nullable=column["is_nullable"] == "YES",
                comment=column["comment"],
                tags=Tag.from_row_tags(column["tags"]),
            )
            for column in (row["columns"] if row["columns"] is not None else [])
        ]


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

    @classmethod
    def from_row(cls, row: Row) -> "TableInfo":
        return TableInfo(
            catalog_name=row["catalog_name"],
            schema_name=row["schema_name"],
            table_name=row["table_name"],
            type=row["table_type"],
            owner=row["owner"],
            comment=row["table_comment"],
            created_at=row["created_at"],
            created_by=row["created_by"],
            updated_at=row["updated_at"],
            updated_by=row["updated_by"],
            data_source_format=row["data_source_format"],
            view_definition=row["view_definition"],
            storage_location=row["storage_path"],
            tags=Tag.from_row_tags(row["tags"]),
            columns=ColumnInfo.from_row(row),
        )


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
