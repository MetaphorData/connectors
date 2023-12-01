from metaphor.kafka.schema_parsers.avro_parser import AvroParser
from tests.test_utils import load_json


def run_test(test_root_dir: str, path: str) -> None:
    with open(f"{test_root_dir}/kafka/schema_parsers/{path}/input.avsc") as f:
        raw_schema = f.read()
    schema = AvroParser.parse(raw_schema, path)
    assert schema is not None
    assert [field.to_dict() for field in schema] == load_json(
        f"{test_root_dir}/kafka/schema_parsers/{path}/expected.json"
    )


def test_parse_schema(test_root_dir) -> None:
    run_test(test_root_dir, "account_event")


def test_simple_union(test_root_dir) -> None:
    run_test(test_root_dir, "simple_union")


def test_union(test_root_dir) -> None:
    run_test(test_root_dir, "union")


def test_nested_arrays(test_root_dir) -> None:
    run_test(test_root_dir, "nested_arrays")


def test_union_of_optional_array(test_root_dir) -> None:
    run_test(test_root_dir, "union_of_optional_array")


def test_complex(test_root_dir) -> None:
    run_test(test_root_dir, "complex")
