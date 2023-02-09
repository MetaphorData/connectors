from datetime import datetime

import pytest
import pytz
from freezegun import freeze_time

from metaphor.common.utils import must_set_exactly_one, start_of_day


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
