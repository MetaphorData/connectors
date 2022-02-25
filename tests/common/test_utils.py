from datetime import datetime

import pytz
from freezegun import freeze_time

from metaphor.common.utils import start_of_day


@freeze_time("2020-01-10")
def test_start_of_day():
    assert start_of_day() == datetime(2020, 1, 10, 0, 0, 0, 0, pytz.UTC)

    assert start_of_day(3) == datetime(2020, 1, 7, 0, 0, 0, 0, pytz.UTC)

    assert start_of_day(30) == datetime(2019, 12, 11, 0, 0, 0, 0, pytz.UTC)
