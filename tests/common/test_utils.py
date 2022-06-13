from datetime import datetime

import pytest
import pytz
from freezegun import freeze_time

from metaphor.common.utils import prepend_at_most_n, start_of_day


@freeze_time("2020-01-10")
def test_start_of_day():
    assert start_of_day() == datetime(2020, 1, 10, 0, 0, 0, 0, pytz.UTC)

    assert start_of_day(3) == datetime(2020, 1, 7, 0, 0, 0, 0, pytz.UTC)

    assert start_of_day(30) == datetime(2019, 12, 11, 0, 0, 0, 0, pytz.UTC)


def test_prepend_at_most_n():
    assert prepend_at_most_n([], 3, 0) == [0]

    assert prepend_at_most_n([1, 2], 3, 0) == [0, 1, 2]

    assert prepend_at_most_n([1, 2, 3], 3, 0) == [0, 1, 2]

    with pytest.raises(Exception):
        prepend_at_most_n([], 0, 0)

    with pytest.raises(Exception):
        prepend_at_most_n([], -1, 0)
