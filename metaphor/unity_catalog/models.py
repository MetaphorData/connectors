import json
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, parse_obj_as, validator

from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import CustomMetadataItem

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


class Table(BaseModel):
    catalog_name: str
    columns: List[Column]
    comment: Optional[str]
    data_source_format: Optional[DataSourceFormat]
    generation: Optional[int]
    name: str
    owner: str
    properties: object
    schema_name: str
    storage_location: Optional[str]
    sql_path: Optional[str]
    table_type: TableType
    updated_at: int
    updated_by: str
    view_definition: Optional[str]

    def extra_metadata(self) -> List[CustomMetadataItem]:
        properties = [
            "data_source_format",
            "generation",
            "owner",
            "properties",
            "storage_location",
            "sql_path",
            "table_type",
        ]
        return [
            CustomMetadataItem(key=p, value=json.dumps(getattr(self, p)))
            for p in filter(lambda p: getattr(self, p, None), properties)
        ]


def parse_table_from_object(obj: object):
    return parse_obj_as(Table, obj)


class NoPermission(BaseModel):
    has_permission: bool = False

    @validator("has_permission")
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
