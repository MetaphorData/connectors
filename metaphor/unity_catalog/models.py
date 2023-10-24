from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, field_validator, parse_obj_as

from metaphor.common.logger import get_logger

logger = get_logger()


class Column(BaseModel):
    name: str
    type_name: str
    type_precision: int
    nullable: bool


class TableType(str, Enum):
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    STREAMING_TABLE = "STREAMING_TABLE"


class DataSourceFormat(str, Enum):
    DELTA = "DELTA"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"
    TEXT = "TEXT"
    UNITY_CATALOG = (
        "UNITY_CATALOG"  # a Table within the Unity Catalog’s Information Schema
    )
    DELTASHARING = "DELTASHARING"  # a Table shared through the Delta Sharing protocol

    def __str__(self):
        return str(self.value)


class Table(BaseModel):
    catalog_name: str
    columns: List[Column]
    comment: Optional[str] = None
    data_source_format: Optional[DataSourceFormat] = None
    generation: Optional[int] = None
    name: str
    owner: str
    properties: dict
    schema_name: str
    storage_location: Optional[str] = None
    sql_path: Optional[str] = None
    table_type: TableType
    updated_at: int
    updated_by: str
    view_definition: Optional[str] = None


def parse_table_from_object(obj: object):
    return parse_obj_as(Table, obj)


class NoPermission(BaseModel):
    has_permission: bool = False

    @field_validator("has_permission")
    @classmethod
    def has_permission_must_be_false(cls, value):
        if value is False:
            return value

        raise ValueError("has_permission must be False")


class LineageColumnInfo(BaseModel):
    name: str
    catalog_name: str
    schema_name: str
    table_name: str


class ColumnLineage(BaseModel):
    upstream_cols: List[Union[LineageColumnInfo, NoPermission]] = []
    downstream_cols: List[Union[LineageColumnInfo, NoPermission]] = []


class FileInfo(BaseModel):
    path: str
    has_permission: bool


class TableInfo(BaseModel):
    name: str
    catalog_name: str
    schema_name: str


class LineageInfo(BaseModel):
    tableInfo: Optional[Union[TableInfo, NoPermission]] = None
    fileInfo: Optional[FileInfo] = None


class TableLineage(BaseModel):
    upstreams: List[LineageInfo] = []
    downstreams: List[LineageInfo] = []
