import json
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, parse_obj_as

from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import CustomMetadataItem

logger = get_logger(__name__)


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
    name: str
    catalog_name: str
    schema_name: str
    table_type: TableType
    data_source_format: Optional[DataSourceFormat]
    columns: List[Column]
    owner: str
    properties: object
    storage_location: Optional[str]
    view_definition: Optional[str]
    sql_path: Optional[str]
    comment: Optional[str]
    updated_at: int
    updated_by: str

    def extra_metadata(self) -> List[CustomMetadataItem]:
        properties = [
            "table_type",
            "data_source_format",
            "owner",
            "properties",
            "storage_location",
            "view_definition",
            "sql_path",
        ]
        return [
            CustomMetadataItem(key=p, value=json.dumps(getattr(self, p)))
            for p in filter(lambda p: getattr(self, p, None), properties)
        ]


def parse_table_from_object(obj: object):
    logger.debug(f"table object: {json.dumps(obj)}")
    return parse_obj_as(Table, obj)
