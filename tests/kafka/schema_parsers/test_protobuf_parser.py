from metaphor.kafka.schema_parsers.protobuf_parser import ProtobufParser
from tests.test_utils import load_json


def run_test(test_root_dir: str, path: str) -> None:
    with open(f"{test_root_dir}/kafka/schema_parsers/{path}/input.proto") as f:
        raw_schema = f.read()
    schema = ProtobufParser.parse(raw_schema, path)
    assert schema is not None
    assert [field.to_dict() for field in schema] == load_json(
        f"{test_root_dir}/kafka/schema_parsers/{path}/expected.json"
    )


def test_address_book(test_root_dir) -> None:
    run_test(test_root_dir, "address_book")


def test_parse_bad_proto() -> None:
    raw_schema = """
syntax = "proto3";
    """
    schema = ProtobufParser.parse(raw_schema, "bad")
    assert schema is None
