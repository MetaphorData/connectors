import pytest
from databricks.sdk.service.catalog import ColumnInfo

from metaphor.unity_catalog.models import (
    DataSourceFormat,
    NoPermission,
    parse_schema_field_from_column_info,
)


def test_no_permission_cannot_have_permission() -> None:
    with pytest.raises(ValueError):
        NoPermission(has_permission=True)


def test_parse_schema_field_from_invalid_column_info() -> None:
    with pytest.raises(ValueError):
        parse_schema_field_from_column_info(ColumnInfo(comment="does not have a type"))


def test_stringify_data_source_format() -> None:
    for fmt in DataSourceFormat:
        assert str(fmt) == fmt.value
