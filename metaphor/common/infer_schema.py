from collections import Counter
from datetime import datetime
from typing import Any
from typing import Counter as CounterType
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union

from typing_extensions import TypedDict

from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import SchemaField

logger = get_logger()

SchemaTypeNameMapping = Dict[Optional[type], str]
"""
Type for mapping from Python types to platform data types.
"""


class _TypeCountSchemaField(TypedDict):
    types: CounterType[Union[type, str]]  # field types and times seen
    count: int  # times the field was seen
    schema_field: SchemaField
    is_array: bool
    subfield_keys: Set[str]


def _is_field_nullable(doc: Dict[str, Any], field_path: Tuple[str, ...]) -> bool:
    """
    Check if a nested field is nullable in a document from a collection.
    """

    if not field_path:
        return True

    field = field_path[0]

    # if field is inside
    if field in doc:
        value = doc[field]

        if value is None:
            return True
        # if no fields left, must be non-nullable
        if len(field_path) == 1:
            return False

        # otherwise, keep checking the nested fields
        remaining_fields = field_path[1:]

        # if dictionary, check additional level of nesting
        if isinstance(value, dict):
            return _is_field_nullable(doc[field], remaining_fields)
        # if list, check if any member is missing field
        if isinstance(value, list):
            # count empty lists of nested objects as nullable
            if len(value) == 0:
                return True
            return any(
                isinstance(x, dict) and _is_field_nullable(x, remaining_fields)
                for x in doc[field]
            )

        # any other types to check?
        # raise ValueError("Nested type not 'list' or 'dict' encountered")
        return True

    return True


def _get_field_native_type(
    field_types: CounterType[Union[type, str]],
    field_path: str,
    type_mapping: SchemaTypeNameMapping,
) -> str:
    field_type: Union[str, type] = "mixed"

    # if single type detected, mark that as the type to go with
    if len(field_types.keys()) == 1:
        field_type = next(iter(field_types))
    elif set(field_types.keys()) == {int, float}:
        field_type = float
    elif set(field_types.keys()) == {datetime, str}:
        field_type = datetime
    else:
        field_type = field_types.most_common(1)[0][0]
        logger.warning(
            f"Multiple types found in field: field = {field_path}, types = {list(field_types.keys())}. Using the most common type {str(field_type)}"
        )

    if isinstance(field_type, str):
        return field_type
    if field_type in type_mapping:
        return type_mapping[field_type]
    logger.warning(f"Unexpected field type: field = {field_path}, type = {field_type}")
    return str(field_type)


def infer_schema(  # noqa: C901
    documents: Sequence[Dict[str, Any]],
    type_mapping: SchemaTypeNameMapping,
) -> List[SchemaField]:
    """
    Infer a schema from documents. The fields and their subfields are sorted by
    `field_name`.
    """

    schema: Dict[Tuple[str, ...], _TypeCountSchemaField] = {}

    def append_to_schema(
        doc: Dict[str, Any],
        prefix: Tuple[str, ...],
    ) -> None:
        """
        Recursively update the schema with a document, which may/may not contain nested fields.
        """

        fields: List[SchemaField] = []

        for key, value in doc.items():

            current_prefix = prefix + (key,)
            field_path = ".".join(current_prefix)
            field = SchemaField(field_path=field_path, field_name=key)
            fields.append(field)

            field_type = type(value)
            is_array = False

            # if nested value, look at the types within
            if isinstance(value, dict):
                append_to_schema(value, current_prefix)
            # if array of values, check what types are within
            if isinstance(value, list):
                is_array = True
                for item in value:
                    # if dictionary, add it as a nested object
                    if isinstance(item, dict):
                        append_to_schema(item, current_prefix)

            # don't record None values (counted towards nullable)
            if value is not None:
                if current_prefix not in schema:
                    schema[current_prefix] = {
                        "types": Counter([field_type]),
                        "count": 1,
                        "schema_field": field,
                        "is_array": is_array,
                        "subfield_keys": set(),
                    }

                else:
                    # update the type count
                    schema[current_prefix]["types"].update({field_type: 1})
                    schema[current_prefix]["count"] += 1

        if prefix and prefix in schema and fields:
            # Only keep track of the keys here
            schema[prefix]["subfield_keys"].update(
                f.field_name for f in fields if f.field_name is not None
            )

    for document in documents:
        append_to_schema(document, ())

    fields: List[SchemaField] = []

    for field_path in schema.keys():
        schema_field = schema[field_path]["schema_field"]
        schema_field.native_type = _get_field_native_type(
            field_types=schema[field_path]["types"],
            field_path=".".join(field_path),
            type_mapping=type_mapping,
        )
        # Now we know every subfield under the current field, let's look them up
        # in the collected schema.
        if schema[field_path]["subfield_keys"]:
            schema_field.subfields = [
                schema[field_path + (key,)]["schema_field"]
                for key in sorted(schema[field_path]["subfield_keys"])
                if (field_path + (key,)) in schema
            ]

        schema_field.nullable = any(
            _is_field_nullable(doc, field_path) for doc in documents
        )

        # If this is a root field in the schema, append it to the return value.
        if len(field_path) == 1:
            fields.append(schema_field)

    return sorted(fields, key=lambda field: field.field_name or "")
