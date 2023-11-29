from metaphor.kafka.schema_parsers.protobuf_parser import ProtobufParser


def test_protobuf_parser(test_root_dir) -> None:
    proto_file = f"{test_root_dir}/kafka/schema_parsers/addressbook.proto"
    with open(proto_file) as f:
        ProtobufParser.parse(f.read())