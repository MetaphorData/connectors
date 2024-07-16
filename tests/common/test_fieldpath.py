from metaphor.common.fieldpath import FieldDataType, build_field_path


def test_build_field_path():
    assert build_field_path("", "a", FieldDataType.PRIMITIVE) == "a"
    assert build_field_path("", "b", FieldDataType.ARRAY) == "b"
    assert build_field_path("", "c", FieldDataType.RECORD) == "c"

    assert build_field_path("F1", "F2", FieldDataType.RECORD) == "f1.f2"
    assert build_field_path("f1", "F.<2>", FieldDataType.ARRAY) == "f1.f%2E%3C2%3E"
    assert (
        build_field_path("f1.f2.f3", "..<<f4>>", FieldDataType.ARRAY)
        == "f1.f2.f3.%2E%2E%3C%3Cf4%3E%3E"
    )
