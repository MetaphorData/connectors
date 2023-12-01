from metaphor.kafka.schema_parsers.protobuf_parser import ProtobufParser
from metaphor.models.metadata_change_event import SchemaField


def test_protobuf_parser(test_root_dir) -> None:
    proto_file = f"{test_root_dir}/kafka/schema_parsers/address_book.proto"
    with open(proto_file) as f:
        schema = ProtobufParser.parse(f.read(), "address_book")
    assert schema == [
        SchemaField(
            field_name="people",
            field_path="people",
            native_type="TYPE_MESSAGE",
            subfields=[
                SchemaField(
                    field_name="name",
                    field_path="people.name",
                    native_type="TYPE_STRING",
                ),
                SchemaField(
                    field_name="id",
                    field_path="people.id",
                    native_type="TYPE_INT32",
                ),
                SchemaField(
                    field_name="email",
                    field_path="people.email",
                    native_type="TYPE_STRING",
                ),
                SchemaField(
                    field_name="phones",
                    field_path="people.phones",
                    native_type="TYPE_MESSAGE",
                    subfields=[
                        SchemaField(
                            field_name="number",
                            field_path="people.phones.number",
                            native_type="TYPE_STRING",
                        ),
                        SchemaField(
                            field_name="type",
                            field_path="people.phones.type",
                            native_type="TYPE_ENUM",
                        ),
                    ],
                ),
                SchemaField(
                    field_name="last_updated",
                    field_path="people.last_updated",
                    native_type="TYPE_MESSAGE",
                    subfields=[
                        SchemaField(
                            field_name="seconds",
                            field_path="people.last_updated.seconds",
                            native_type="TYPE_INT64",
                        ),
                        SchemaField(
                            field_name="nanos",
                            field_path="people.last_updated.nanos",
                            native_type="TYPE_INT32",
                        ),
                    ],
                ),
            ],
        )
    ]


def test_parse_bad_proto() -> None:
    raw_schema = """
syntax = "proto3";
    """
    schema = ProtobufParser.parse(raw_schema, "bad")
    assert schema is None
