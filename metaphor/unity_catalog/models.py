from enum import Enum
from typing import List, Optional, Union

from databricks.sdk.service.catalog import ColumnInfo
from pydantic import BaseModel, field_validator

from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import SchemaField

logger = get_logger()


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


def extract_schema_field_from_column_info(column: ColumnInfo) -> SchemaField:
    if column.type_name is None:
        raise ValueError(f"Invalid column {column.name}, no type_name found")
    return SchemaField(
        subfields=None,
        field_name=column.name,
        field_path=column.name,
        native_type=column.type_name.value.lower(),
        precision=(
            float(column.type_precision)
            if column.type_precision is not None
            else float("nan")
        ),
        description=column.comment,
    )


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
