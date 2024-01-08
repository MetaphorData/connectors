from datetime import datetime

import pytest

from metaphor.s3.path_spec import PartitionField
from metaphor.s3.table_data import TableData


def test_merge_table_data() -> None:
    foo = TableData(
        display_name="foo",
        full_path="foo",
        partitions=None,
        timestamp=datetime(2023, 1, 1, 0, 0, 0, 0),
        table_path="foo",
        size_in_bytes=1234,
        number_of_files=1,
    )
    bar = TableData(
        display_name="bar",
        full_path="bar",
        partitions=None,
        timestamp=datetime(2024, 1, 1, 0, 0, 0, 0),
        table_path="foo",
        size_in_bytes=5678,
        number_of_files=1,
    )

    merged = foo.merge(bar)

    assert merged == TableData(
        display_name="foo",
        full_path="bar",
        partitions=None,
        timestamp=datetime(2024, 1, 1, 0, 0, 0, 0),
        table_path="foo",
        size_in_bytes=5678,
        number_of_files=1,
    )


def test_merge_table_data_with_partitions() -> None:
    foo = TableData(
        display_name="foo",
        full_path="foo",
        partitions=[
            PartitionField(name="a", raw_value="1234", index=0),
            PartitionField(name="b", raw_value="value", index=1),
        ],
        timestamp=datetime(2023, 1, 1, 0, 0, 0, 0),
        table_path="foo",
        size_in_bytes=1234,
        number_of_files=1,
    )
    bar = TableData(
        display_name="bar",
        full_path="bar",
        partitions=[
            PartitionField(name="a", raw_value="another_value", index=0),
            PartitionField(name="b", raw_value="1.2345", index=1),
        ],
        timestamp=datetime(2024, 1, 1, 0, 0, 0, 0),
        table_path="foo",
        size_in_bytes=5678,
        number_of_files=1,
    )

    merged = foo.merge(bar)
    assert merged == TableData(
        display_name="foo",
        full_path="bar",
        partitions=[
            PartitionField(name="a", raw_value="another_value", index=0),
            PartitionField(name="b", raw_value="value", index=1),
        ],
        timestamp=datetime(2024, 1, 1, 0, 0, 0, 0),
        table_path="foo",
        size_in_bytes=6912,
        number_of_files=2,
    )


def test_merge_table_data_with_different_paths() -> None:
    foo = TableData(
        display_name="foo",
        full_path="foo",
        partitions=None,
        timestamp=datetime(2023, 1, 1, 0, 0, 0, 0),
        table_path="foo",
        size_in_bytes=1234,
        number_of_files=1,
    )
    bar = TableData(
        display_name="bar",
        full_path="bar",
        partitions=None,
        timestamp=datetime(2024, 1, 1, 0, 0, 0, 0),
        table_path="bar",
        size_in_bytes=5678,
        number_of_files=1,
    )

    with pytest.raises(ValueError):
        foo.merge(bar)
