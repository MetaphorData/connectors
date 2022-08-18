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
    path = parent_field_path
    field_name_encoded = (
        field_name.replace(".", "%2E").replace("<", "%3C").replace(">", "%3E")
    )

    if field_type in (
        FieldDataType.PRIMITIVE,
        FieldDataType.RECORD,
        FieldDataType.ARRAY,
    ):
        path = f"{path}.{field_name_encoded}"
    else:
        raise ValueError(f"Unsupported field data type {field_type}")

    # remove prefix "." if parent path is emtpy string (top-level field)
    return path if parent_field_path != "" else path[1:]
