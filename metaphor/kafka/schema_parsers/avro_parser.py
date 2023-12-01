from typing import List, Optional, Tuple

import avro.schema as avroschema
from avro.schema import ArraySchema, RecordSchema, UnionSchema

from metaphor.common.logger import get_logger
from metaphor.kafka.schema_parsers.utils import get_field_path
from metaphor.models.metadata_change_event import SchemaField

logger = get_logger()


class AvroParser:
    def __init__(self) -> None:
        pass

    @staticmethod
    def _safe_get_name(schema: avroschema.Schema) -> str:
        prop_name = schema.get_prop("name")
        if prop_name is None:
            return "<NA>"
        return str(prop_name)

    @staticmethod
    def _safe_get_doc(schema: avroschema.Schema) -> Optional[str]:
        return (
            str(schema.get_prop("doc")) if schema.get_prop("doc") is not None else None
        )

    def _parse_array_child(
        self,
        arr_item: avroschema.Schema,
        cur_path: str,
    ) -> Tuple[str, List[SchemaField]]:
        """
        Returns the type in the array along with the list of schema fields for the underlying
        complex types.
        """
        if isinstance(arr_item, ArraySchema):
            display_type, children = self._parse_array_child(arr_item.items, cur_path)
            return f"ARRAY<{display_type}>", children

        if isinstance(arr_item, UnionSchema):
            display_type, children = self._parse_union_children(
                union_schema=arr_item,
                cur_path=cur_path,
            )
            return f"UNION<{display_type}>", children

        if isinstance(arr_item, RecordSchema):
            record_child = self._parse_record_child(arr_item, cur_path)
            return str(arr_item.type), [record_child]

        return str(arr_item.type), []

    def _parse_record_child(
        self,
        record_schema: RecordSchema,
        cur_path: str,
    ) -> SchemaField:
        """
        Parses the record schema into a schema field.
        """
        field_path = get_field_path(cur_path, record_schema.name)
        child_field = SchemaField(
            field_name=record_schema.name,
            field_path=field_path,
            native_type=str(record_schema.type).upper(),
            subfields=self.get_avro_fields(record_schema, field_path),
            description=record_schema.doc,
        )
        return child_field

    def _parse_union_children(
        self,
        union_schema: UnionSchema,
        cur_path: str,
    ) -> Tuple[str, List[SchemaField]]:
        """
        Returns the union types along with the list of schema fields for the underlying
        complex types.

        Note that avro specification for union schemas is that an union cannot immediately
        contain another union, so here we do not handle that.
        """
        sub_type = ",".join(str(schema.type) for schema in union_schema.schemas)
        subfields = []
        for field in union_schema.schemas:
            if isinstance(field, ArraySchema):
                _, arr_children = self._parse_array_child(field.items, cur_path)
                if arr_children:
                    subfields.extend(arr_children)
            if isinstance(field, RecordSchema):
                record_child = self._parse_record_child(field, cur_path)
                subfields.append(record_child)

        return sub_type, subfields

    def parse_array_fields(
        self,
        field: ArraySchema,
        cur_path: str,
    ) -> SchemaField:
        """
        Parse array field for avro schema

        In case of a simple array of primitive type,
        for example {"type":"array","items":"string"}
        the display type would be `ARRAY<string>`

        If it's a nested array the we will parse
        the nested array as well
        for example {"type":"array","items":{"type":"array","items":"string"}}
        the display type would be `ARRAY<ARRAY<string>>`

        If it's a array contains a record as an item it will be added as child of array
        the nested array as well
        for example {
                    "type":"array",
                    "items":{
                        "type":"array",
                        "items":{
                            "name":"test",
                            "type":"record",
                            "fields":[{"name":"id","type":"int"}]
                        }
                    }
        the display type would be `ARRAY<ARRAY<record>>`
        """
        field_name = AvroParser._safe_get_name(field)
        field_path = get_field_path(cur_path, field_name)
        schema_field = SchemaField(
            field_name=field_name,
            field_path=field_path,
            native_type=str(field.type.type).upper(),  # type: ignore # avro types are broken, field.type is actually a schema
            description=AvroParser._safe_get_doc(field),
        )

        array_type, subfields = self._parse_array_child(
            arr_item=field.type.items,  # type: ignore # avro types are broken, field.type is actually a schema
            cur_path=get_field_path(cur_path, field_name),
        )

        schema_field.native_type = f"{schema_field.native_type}<{array_type}>"
        if subfields:
            schema_field.subfields = subfields
        return schema_field

    def parse_record_fields(self, field: RecordSchema, cur_path: str) -> SchemaField:
        """
        Parse the nested record fields for avro
        """
        field_path = get_field_path(cur_path, field.name)
        child_schema: RecordSchema = field.type  # type: ignore # avro types are broken, field.type is actually a record schema
        schema_field = SchemaField(
            field_name=field.name,
            field_path=field_path,
            native_type="RECORD",
            subfields=[self._parse_record_child(child_schema, field_path)],
            description=field.doc,
        )
        return schema_field

    def parse_union_fields(
        self,
        union_field: avroschema.Schema,
        cur_path: str,
    ) -> SchemaField:
        """
        Parse union field for avro schema.

        Here we populate the schema field for the union schema, and we'll parse
        the underlying types (arrays or records) in `_parse_union_children`.
        """

        field_type: UnionSchema = union_field.type  # type: ignore # avro types are broken, field.type is actually a union schema
        field_name = AvroParser._safe_get_name(union_field)
        field_path = get_field_path(cur_path, field_name)
        schema_field = SchemaField(
            field_name=field_name,
            field_path=field_path,
            native_type=str(field_type.type).upper(),  # This is UNION
            description=AvroParser._safe_get_doc(union_field),
        )
        union_types, children = self._parse_union_children(
            field_type,
            field_path,
        )
        schema_field.native_type = f"{schema_field.native_type}<{union_types}>"
        if children:
            schema_field.subfields = children
        return schema_field

    def parse_single_field(
        self,
        field: avroschema.Schema,
        cur_path: str,
    ) -> SchemaField:
        """
        Parses primitive fields.
        """
        field_name = AvroParser._safe_get_name(field)
        return SchemaField(
            field_name=field_name,
            field_path=get_field_path(cur_path, field_name),
            native_type=str(field.type.type).upper(),  # type: ignore # avro types are broken, field.type is actually a schema
            description=AvroParser._safe_get_doc(field),
        )

    def get_avro_fields(
        self,
        parsed_schema: avroschema.Schema,
        cur_path: str = "",
    ) -> List[SchemaField]:
        """
        The main recursion. Recursively convert parsed schemas into schema field models.
        """
        if parsed_schema.get_prop("fields") is None:
            # No field to parse
            return []

        field_models = []

        for field in parsed_schema.fields:  # type: ignore # if field is a valid prop then it will be a list of schemas
            # Note: for union schema and array schema types, the actual class instance
            # is in `field.type`, not `field` itself.
            if isinstance(field.type, ArraySchema):
                field_models.append(self.parse_array_fields(field, cur_path))
            elif isinstance(field.type, UnionSchema):
                field_models.append(self.parse_union_fields(field, cur_path))
            elif isinstance(field.type, RecordSchema):
                field_models.append(self.parse_record_fields(field, cur_path))
            else:
                field_models.append(self.parse_single_field(field, cur_path))
        return field_models

    def run_parser(self, raw_schema: str) -> List[SchemaField]:
        parsed_schema = avroschema.parse(raw_schema)
        field_name = AvroParser._safe_get_name(parsed_schema)
        models = [
            SchemaField(
                field_name=field_name,
                field_path=field_name,
                native_type=str(parsed_schema.type).upper(),
                subfields=self.get_avro_fields(parsed_schema, field_name),
                description=AvroParser._safe_get_doc(parsed_schema),
            )
        ]
        return models

    @staticmethod
    def parse(raw_schema: str, subject: str) -> Optional[List[SchemaField]]:
        try:
            return AvroParser().run_parser(raw_schema)
        except Exception:
            logger.exception(f"Failed to parse schema for subject: {subject}")
        return None
