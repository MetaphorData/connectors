from enum import Enum


class FieldDataType(Enum):
    PRIMITIVE = "PRIMITIVE"
    RECORD = "RECORD"
    ARRAY = "ARRAY"
    MAP = "MAP"
    UNION = "UNION"


def build_field_path(
    parent_field_path: str, field_name: str, field_type: FieldDataType
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
