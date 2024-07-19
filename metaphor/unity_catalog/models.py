from typing import Dict, List

from databricks.sdk.service.catalog import ColumnInfo
from pydantic import BaseModel

from metaphor.common.fieldpath import build_schema_field
from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import SchemaField

logger = get_logger()


def extract_schema_field_from_column_info(column: ColumnInfo) -> SchemaField:
    if column.name is None or column.type_name is None:
        raise ValueError(f"Invalid column {column.name}, no type_name found")

    field = build_schema_field(
        column.name, column.type_name.value.lower(), column.comment
    )
    field.precision = (
        float(column.type_precision)
        if column.type_precision is not None
        else float("nan")
    )
    return field


class TableLineage(BaseModel):
    upstream_tables: List[str] = []


class Column(BaseModel):
    column_name: str
    table_name: str


class ColumnLineage(BaseModel):
    upstream_columns: Dict[str, List[Column]] = {}
