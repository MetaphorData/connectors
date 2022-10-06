from datetime import datetime, time, timedelta, timezone
from hashlib import md5
from typing import Dict, List


def start_of_day(daysAgo=0) -> datetime:
    """Returns the start of day in UTC time, for today or N days ago"""
    return datetime.combine(
        datetime.now().date(), time(tzinfo=timezone.utc)
    ) - timedelta(days=daysAgo)


def unique_list(non_unique_list: list) -> list:
    """Returns an order-preserving list with no duplicate elements"""
    return list(dict.fromkeys(non_unique_list))


def chunks(list, n):
    """Yield successive n-sized chunks from the list."""
    for i in range(0, len(list), n):
        yield list[i : i + n]


def must_set_exactly_one(values: Dict, keys: List[str]):
    not_none = [k for k in keys if values.get(k) is not None]
    if len(not_none) != 1:
        raise ValueError(f"must set exactly one of {keys}, found {not_none}")


def md5_digest(value: bytes) -> str:
    """For computing non-crypto use of MD5 digest"""
    return md5(value).hexdigest()  # nosec B303, B324
