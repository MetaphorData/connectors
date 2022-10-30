import json
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, parse_obj_as

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
    logger.debug(f"table object: {json.dumps(obj)}")
    return parse_obj_as(Table, obj)


class LineageColumnInfo(BaseModel):
    name: str
    catalog_name: str
    schema_name: str
    table_name: str


class ColumnLineage(BaseModel):
    upstream_cols: List[LineageColumnInfo] = []
    downstream_cols: List[LineageColumnInfo] = []


class TableInfo(BaseModel):
    name: str
    catalog_name: str
    schema_name: str


class LineageInfo(BaseModel):
    tableInfo: TableInfo


class TableLineage(BaseModel):
    upstreams: List[LineageInfo] = []
    downstreams: List[LineageInfo] = []
