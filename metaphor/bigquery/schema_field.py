from typing import Any, List, Mapping, Sequence, Union

import google.cloud.bigquery.schema

from metaphor.common.fieldpath import FieldDataType, build_field_path
from metaphor.models.metadata_change_event import SchemaField


class SchemaFieldExtractor:
    @staticmethod
    def to_native_type(field: google.cloud.bigquery.schema.SchemaField) -> str:
        """
        Extracts the native type from a BigQuery schema field.
        """
        # mode REPEATED means ARRAY
        return (
            f"ARRAY<{field.field_type}>"
            if field.mode == "REPEATED"
            else str(field.field_type)
        )

    @staticmethod
    def to_field_data_type(
        field: google.cloud.bigquery.schema.SchemaField,
    ) -> FieldDataType:
        """
        Extracts the field data type from a BigQuery schema field.
        """
        # See https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type
        return (
            FieldDataType.ARRAY
            if field.mode == "REPEATED"
            else (
                FieldDataType.RECORD
                if field.field_type in ("RECORD", "STRUCT")
                else FieldDataType.PRIMITIVE
            )
        )

    @staticmethod
    def to_field_path(
        field: google.cloud.bigquery.schema.SchemaField, parent_field_path: str
    ) -> str:
        """
        Extracts the field path from a BigQuery schema field.
        """
        return build_field_path(
            parent_field_path,
            field.name,
            SchemaFieldExtractor.to_field_data_type(field),
        )

    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.schema.SchemaField.html#google.cloud.bigquery.schema.SchemaField
    @staticmethod
    def parse_fields(
        schema: Sequence[
            Union[google.cloud.bigquery.schema.SchemaField, Mapping[str, Any]]
        ],
        parent_field_path: str,
    ) -> List[SchemaField]:
        """
        Recursively parses Metaphor fields from a sequence of SchemaFields.
        """
        fields: List[SchemaField] = []
        for field in schema:
            # There's no documentation on how to handle the Mapping[str, Any] type.
            # Actual API also doesn't seem to return this type: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
            if not isinstance(field, google.cloud.bigquery.schema.SchemaField):
                raise ValueError(f"Field type not supported: {field}")

            field_path = SchemaFieldExtractor.to_field_path(field, parent_field_path)

            subfields = None
            if field.fields is not None and len(field.fields) > 0:
                subfields = SchemaFieldExtractor.parse_fields(field.fields, field_path)

            fields.append(
                SchemaField(
                    field_path=field_path,
                    field_name=field.name,
                    description=field.description,
                    native_type=SchemaFieldExtractor.to_native_type(field),
                    nullable=field.is_nullable,
                    subfields=subfields,
                )
            )

        return fields
