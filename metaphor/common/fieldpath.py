from enum import Enum
from typing import Optional

from metaphor.models.metadata_change_event import SchemaField


class FieldDataType(Enum):
    PRIMITIVE = "PRIMITIVE"
    RECORD = "RECORD"
    ARRAY = "ARRAY"
    MAP = "MAP"
    UNION = "UNION"


def build_field_path(
    parent_field_path: str,
    field_name: str,
    field_type: FieldDataType = FieldDataType.PRIMITIVE,
) -> str:
    """
    Build field path for nested field based on parent field path and field type.
    Field path will be normalized to lowercase
    """
    field_name_encoded = (
        field_name.lower().replace(".", "%2E").replace("<", "%3C").replace(">", "%3E")
    )

    if field_type in (
        FieldDataType.PRIMITIVE,
        FieldDataType.RECORD,
        FieldDataType.ARRAY,
    ):
        return (
            f"{parent_field_path.lower()}.{field_name_encoded}"
            if parent_field_path
            else field_name_encoded
        )
    else:
        raise ValueError(f"Unsupported field data type {field_type}")


def build_schema_field(
    column_name: str,
    field_type: Optional[str] = None,
    description: Optional[str] = None,
    nullable: Optional[bool] = None,
    field_path: Optional[str] = None,
    precision: Optional[float] = None,
) -> SchemaField:
    """
    Build a schema field for a simple (non-nested) field based on column information.
    If no "field_path" is specified, it will use `column_name.lower()`
    """
    return SchemaField(
        field_name=column_name,
        field_path=field_path or column_name.lower(),
        native_type=field_type or None,
        description=description or None,
        nullable=nullable,
        precision=precision,
    )
