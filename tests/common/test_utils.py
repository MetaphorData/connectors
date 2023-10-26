from datetime import datetime

import pytest
import pytz
from freezegun import freeze_time

from metaphor.common.utils import (
    chunk_by_size,
    filter_empty_strings,
    filter_none,
    is_email,
    must_set_at_least_one,
    must_set_exactly_one,
    removesuffix,
    safe_float,
    safe_int,
    safe_parse_ISO8601,
    safe_str,
    start_of_day,
    unique_list,
)


@freeze_time("2020-01-10")
def test_start_of_day():
    assert start_of_day() == datetime(2020, 1, 10, 0, 0, 0, 0, pytz.UTC)

    assert start_of_day(3) == datetime(2020, 1, 7, 0, 0, 0, 0, pytz.UTC)

    assert start_of_day(30) == datetime(2019, 12, 11, 0, 0, 0, 0, pytz.UTC)


def test_must_set_exactly_one():
    must_set_exactly_one({"foo": 1}, ["foo"])
    must_set_exactly_one({"foo": 1, "bar": None}, ["foo", "bar"])
    must_set_exactly_one({"bar": 1, "baz": None}, ["foo", "bar", "baz"])

    with pytest.raises(ValueError):
        must_set_exactly_one({}, ["foo"])

    with pytest.raises(ValueError):
        must_set_exactly_one({"foo": 1, "bar": 1}, ["foo", "bar"])


def test_must_set_at_least_one():
    must_set_at_least_one({"foo": 1}, ["foo"])
    must_set_at_least_one({"foo": 1, "bar": None}, ["foo", "bar"])
    must_set_at_least_one({"foo": 1, "bar": 42}, ["foo", "bar"])
    must_set_at_least_one({"bar": 1, "baz": None}, ["foo", "bar", "baz"])

    with pytest.raises(ValueError):
        must_set_at_least_one({}, ["foo"])

    with pytest.raises(ValueError):
        must_set_at_least_one({"foo": 1, "bar": 1}, ["baz"])


def test_filter_empty_strings():
    assert filter_empty_strings(["", "", ""]) == []
    assert filter_empty_strings(["foo", "", "bar"]) == ["foo", "bar"]
    assert filter_empty_strings(["foo", "bar", "baz"]) == ["foo", "bar", "baz"]


def test_chunk_by_size():
    # Return length of item as size
    def size_func(item):
        return len(item)

    # Each item fits into a chunk exactly
    assert chunk_by_size(["a", "b", "c"], 10, 1, size_func) == [
        slice(0, 1),  # ['a']
        slice(1, 2),  # ['b']
        slice(2, 3),  # ['c']
    ]

    # Each item is smaller than chunk_size but adjacent pairs are too large
    assert chunk_by_size(["aa", "bb", "cc"], 10, 3, size_func) == [
        slice(0, 1),  # ['aa']
        slice(1, 2),  # ['bb']
        slice(2, 3),  # ['cc']
    ]

    # Each item is larger than chunk_size
    assert chunk_by_size(["aa", "bb", "cc"], 10, 1, size_func) == [
        slice(0, 1),  # ['aa']
        slice(1, 2),  # ['bb']
        slice(2, 3),  # ['cc']
    ]

    # Each pair of items adds up to the chunk size
    assert chunk_by_size(["a", "b", "c", "d", "e"], 10, 2, size_func) == [
        slice(0, 2),  # ['a', 'b']
        slice(2, 4),  # ['c', 'd']
        slice(4, 5),  # ['e']
    ]

    # Can fit all items into a chunk but limited by items_per_chunk
    assert chunk_by_size(["a", "b", "c", "d", "e"], 3, 10, size_func) == [
        slice(0, 3),  # ['a', 'b', 'c']
        slice(3, 5),  # ['d', 'e']
    ]

    # A complex case that limits the chunk by both size & count
    assert chunk_by_size(["aa", "b", "c", "d", "eee", "ffff"], 2, 3, size_func) == [
        slice(0, 2),  # ['aa', 'b']
        slice(2, 4),  # ['c', 'd']
        slice(4, 5),  # ['eee']
        slice(5, 6),  # ['ffff']
    ]


def test_unique_list():
    assert unique_list(["a", "b", "c"]) == ["a", "b", "c"]
    assert unique_list(["a", "a", "c"]) == ["a", "c"]
    assert unique_list(["c", "a", "c"]) == ["c", "a"]


def test_remove_suffix():
    assert removesuffix("abcdefg", "fg") == "abcde"
    assert removesuffix("abcdefg", "gf") == "abcdefg"
    assert removesuffix("example.com/index.html", "index.html") == "example.com/"
    assert removesuffix("example.com/index.html", "/index.html") == "example.com"


def test_safe_parse_ISO8061():
    assert safe_parse_ISO8601(None) is None
    assert (
        safe_parse_ISO8601("2023-09-20T08:10:15Z").isoformat()
        == "2023-09-20T08:10:15+00:00"
    )
    assert (
        safe_parse_ISO8601("2023-09-20T08:10:15").isoformat()
        == "2023-09-20T08:10:15+00:00"
    )
    assert safe_parse_ISO8601("isvalid") is None


def test_safe_str():
    assert safe_str(None) is None
    assert safe_str("string") == "string"
    assert safe_str(100) == "100"


def test_safe_float():
    assert safe_float(None) is None
    assert safe_float(float("NaN")) is None
    assert safe_float(float("Inf")) is None
    assert safe_float(1) == 1.0
    assert safe_float(1.7) == 1.7


def test_safe_int():
    assert safe_int(None) is None
    assert safe_float(float("NaN")) is None
    assert safe_float(float("Inf")) is None
    assert safe_int(1) == 1
    assert safe_int(1.7) == 1


def test_filter_none():
    assert filter_none([]) == []
    assert filter_none([1, 2]) == [1, 2]
    assert filter_none(["foo", None, "bar"]) == ["foo", "bar"]
    assert filter_none([None, None]) == []


def test_is_email():
    assert is_email("a@b.c")
    assert is_email("a+c@b.c")
    assert not is_email("foo")
    assert not is_email("foo@127.0.0.1")
