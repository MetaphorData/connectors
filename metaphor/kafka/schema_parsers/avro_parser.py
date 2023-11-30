from typing import List, Optional, Tuple

import avro.schema as avroschema
from avro.schema import ArraySchema, RecordSchema, UnionSchema

from metaphor.common.logger import get_logger
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

    @staticmethod
    def _get_field_path(cur_path: str, field_name: str) -> str:
        field_path = ".".join([cur_path, field_name])
        if cur_path == "":
            field_path = field_path[1:]
        return field_path

    def _parse_array_children(
        self,
        arr_item: avroschema.Schema,
        cur_path: str,
    ) -> Tuple[str, Optional[SchemaField]]:
        if isinstance(arr_item, ArraySchema):
            display_type, children = self._parse_array_children(
                arr_item.items, cur_path
            )
            return f"ARRAY<{display_type}>", children

        if isinstance(arr_item, UnionSchema):
            display_type, children = self._parse_union_children(
                parent=None,
                union_field=arr_item,
                cur_path=cur_path,
            )
            return f"UNION<{display_type}>", children

        if isinstance(arr_item, RecordSchema):
            child_obj = SchemaField(
                field_name=arr_item.name,
                field_path=AvroParser._get_field_path(cur_path, arr_item.name),
                native_type=str(arr_item.type).upper(),
                subfields=self.get_avro_fields(arr_item, cur_path),
                description=arr_item.doc,
            )
            return str(arr_item.type), child_obj

        return str(arr_item.type), None

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
        field_path = AvroParser._get_field_path(cur_path, field_name)
        schema_field = SchemaField(
            field_name=field_name,
            field_path=field_path,
            native_type=str(field.type.type).upper(),  # type: ignore # avro types are broken, field.type is actually a schema
            description=AvroParser._safe_get_doc(field),
        )

        _, subfields = self._parse_array_children(
            arr_item=field.type.items,  # type: ignore # avro types are broken, field.type is actually a schema
            cur_path=AvroParser._get_field_path(cur_path, field_name),
        )

        if subfields:
            schema_field.subfields = [subfields]
        return schema_field

    def _parse_union_children(
        self,
        parent: Optional[avroschema.Schema],
        union_field: UnionSchema,
        cur_path: str,
    ) -> Tuple[str, Optional[SchemaField]]:
        non_null_schema = [
            (i, schema)
            for i, schema in enumerate(union_field.schemas)
            if schema.type != "null"
        ]
        sub_type = ",".join(str(schema.type) for schema in union_field.schemas)
        if len(union_field.schemas) == 2 and len(non_null_schema) == 1:
            field = non_null_schema[0][1]

            if isinstance(field, ArraySchema):
                display, children = self._parse_array_children(
                    arr_item=field.items, cur_path=cur_path
                )
                sub_types: List[str] = ["", ""]
                sub_types[non_null_schema[0][0]] = f"ARRAY<{display}>"
                sub_types[non_null_schema[0][0] ^ 1] = "null"
                return ",".join(sub_types), children

            # if the child is a recursive instance of parent we will only process it once
            if isinstance(field, RecordSchema):
                field_path = AvroParser._get_field_path(cur_path, field.name)
                children = SchemaField(
                    field_name=field.name,
                    field_path=field_path,
                    native_type=str(field.type).upper(),
                    subfields=None
                    if field == parent
                    else self.get_avro_fields(field, field_path),
                    description=field.doc,
                )
                return sub_type, children

        return sub_type, None

    def parse_record_fields(self, field: RecordSchema, cur_path: str) -> SchemaField:
        """
        Parse the nested record fields for avro
        """
        field_path = AvroParser._get_field_path(cur_path, field.name)
        child_schema: RecordSchema = field.type  # type: ignore # avro types are broken, field.type is actually a record schema
        child_path = AvroParser._get_field_path(field_path, child_schema.name)
        schema_field = SchemaField(
            field_name=field.name,
            field_path=field_path,
            native_type="RECORD",
            subfields=[
                SchemaField(
                    field_name=child_schema.name,
                    field_path=child_path,
                    native_type="RECORD",
                    subfields=self.get_avro_fields(child_schema, child_path),
                    description=child_schema.doc,
                )
            ],
            description=field.doc,
        )
        return schema_field

    def parse_union_fields(
        self,
        parent: Optional[avroschema.Schema],
        union_field: avroschema.Schema,
        cur_path: str,
    ) -> SchemaField:
        """
        Parse union field for avro schema

        While parsing the union field there are couple of possibilities
        if we are parsing union of primitive types for example:
            ["null","int","string"]
        the expected display type would be:
            UNION<null,int,string>

        if we have a special case of union of a null with a complex type like array
        this function would parse the complex type and accordingly prepare the display type
        in the same case if the complex type is record, it will be added as a children.

        for example: ["null", {"name":"test","type":"record","fields":[{"name":"id","type":"int"}]}]
        then the record "test" would be added as a child and the display type would be UNION<null,record>
        see `test_avro_parser.py` for more example


        If the union schema contains multiple complex type in that case
        we will not be able to parse it and in the display type the function just records
        the type of top level element

        for example: [
                        "null",
                        {"type":"array","items":"int"} ,
                        {"name":"test","type":"record","fields":[{"name":"id","type":"int"}]}
                    ]
        even though we have a record in union schema it will not be added as child
        and the display type would be: UNION<null,array,record>
        """

        field_type: UnionSchema = union_field.type  # type: ignore # avro types are broken, field.type is actually a union schema
        field_name = AvroParser._safe_get_name(union_field)
        field_path = AvroParser._get_field_path(cur_path, field_name)
        schema_field = SchemaField(
            field_name=field_name,
            field_path=field_path,
            native_type=str(field_type.type).upper(),
            description=AvroParser._safe_get_doc(union_field),
        )
        _, children = self._parse_union_children(
            parent,
            field_type,
            field_path,
        )
        if children:
            schema_field.subfields = [children]
        return schema_field

    def parse_single_field(
        self,
        field: avroschema.Schema,
        cur_path: str,
    ) -> SchemaField:
        """
        Parse primitive field for avro schema
        """
        field_name = AvroParser._safe_get_name(field)
        return SchemaField(
            field_name=field_name,
            field_path=AvroParser._get_field_path(cur_path, field_name),
            native_type=str(field.type.type).upper(),  # type: ignore # avro types are broken, field.type is actually a schema
            description=AvroParser._safe_get_doc(field),
        )

    def run_parser(self, raw_schema: str) -> List[SchemaField]:
        """
        Method to parse the avro schema
        """
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

    def get_avro_fields(
        self,
        parsed_schema: avroschema.Schema,
        cur_path: str = "",
    ) -> List[SchemaField]:
        """
        Recursively convert the parsed schema into required models
        """
        if parsed_schema.get_prop("fields") is None:
            # No field to parse
            return []

        field_models = []

        for field in parsed_schema.fields:  # type: ignore # if field is a valid prop then it will be a list of schemas
            if isinstance(field.type, ArraySchema):
                field_models.append(self.parse_array_fields(field, cur_path))
            elif isinstance(field.type, UnionSchema):
                field_models.append(
                    self.parse_union_fields(parsed_schema, field, cur_path)
                )
            elif isinstance(field.type, RecordSchema):
                field_models.append(self.parse_record_fields(field, cur_path))
            else:
                field_models.append(self.parse_single_field(field, cur_path))
        return field_models

    @staticmethod
    def parse(raw_schema: str, subject: str) -> Optional[List[SchemaField]]:
        try:
            return AvroParser().run_parser(raw_schema)
        except Exception:
            logger.exception(f"Failed to parse schema for subject: {subject}")
        return None
