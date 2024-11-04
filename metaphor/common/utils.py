import math
from datetime import datetime, time, timedelta, timezone
from hashlib import md5
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union

from dateutil.parser import isoparse
from pydantic import validate_email

from metaphor.common.logger import get_logger

logger = get_logger()


def start_of_day(daysAgo=0) -> datetime:
    """Returns the start of day in UTC time, for today or N days ago"""
    return datetime.combine(
        datetime.now().date(), time(tzinfo=timezone.utc)
    ) - timedelta(days=daysAgo)


T = TypeVar("T")


def unique_list(non_unique_list: Iterable[T]) -> list[T]:
    """Returns an order-preserving list with no duplicate elements"""
    return list(dict.fromkeys(non_unique_list))


def dedup_lists(left: Optional[List[T]], right: Optional[List[T]]) -> List[T]:
    """
    Dedups two lists into a single list. Order is preserved.
    """
    return unique_list((left or []) + (right or []))


def filter_empty_strings(original_list: list) -> list:
    return list(filter(lambda e: len(e) > 0, original_list))


def chunks(list, n):
    """Yield successive n-sized chunks from the list."""
    for i in range(0, len(list), n):
        yield list[i : i + n]


def must_set_exactly_one(values: Dict, keys: List[str]):
    not_none = [k for k in keys if values.get(k) is not None]
    if len(not_none) != 1:
        raise ValueError(f"Must set exactly one of {keys}, found {not_none}")


def must_set_at_least_one(values: Dict, keys: List[str]):
    if not next((k for k in keys if values.get(k) is not None), None):
        raise ValueError(f"Must set at least one of {keys}")


def md5_digest(value: bytes) -> str:
    """For computing non-crypto use of MD5 digest"""
    return md5(value).hexdigest()  # nosec B303, B324


def generate_querylog_id(platform: str, id: str) -> str:
    """Generate queryLog id"""
    return f"{platform}:{id}"


def to_utc_time(time: Optional[datetime]) -> Optional[datetime]:
    """convert local datetime to utc timezone"""
    if time is not None:
        return time.replace(tzinfo=timezone.utc)
    return None


def safe_parse_ISO8601(iso8601_str: Optional[str]) -> Optional[datetime]:
    """Safely convert ISO 8601 string to UTC datetime"""
    if iso8601_str is None:
        return None
    try:
        return isoparse(iso8601_str).replace(tzinfo=timezone.utc)
    except Exception:
        logger.error(f"Failed to parse ISO8061 time: {iso8601_str}")
        return None


def safe_str(value: Any) -> Optional[str]:
    """Converts a value to str, return None if the original value is None"""
    return None if value is None else str(value)


def non_empty_str(value: Any) -> Optional[str]:
    """Converts a value to str, return None if the original value is None or empty string"""
    return None if value is None or value == "" else str(value)


def safe_float(value: Optional[Union[float, int, str]]) -> Optional[float]:
    """Converts a value to float, return None if the original value is None or NaN or INF"""
    return (
        None
        if value is None
        or (isinstance(value, float) and (math.isnan(value) or math.isinf(value)))
        else float(value)
    )


def safe_int(value: Any) -> Optional[int]:
    """Converts a value to int, return None if the original value is None or NaN or INF"""
    return (
        None
        if value is None
        or (isinstance(value, float) and (math.isnan(value) or math.isinf(value)))
        else int(value)
    )


def chunk_by_size(
    list_to_chunk: list,
    items_per_chunk: int,
    chunk_size: int,
    size_func: Callable[[Any], int],
) -> List[slice]:
    """Split a list into chunks based on specified limits

    Normally each chunk is packed with as many successive items as possible
    without exceeding items_per_chunk & chunk_size. However, if a single item
    is larger than chunk_size, it'll be put into its own chunk.

    Parameters
    ----------
    items_per_chunk:
        Maximum number of items in a chunk
    list_to_chunk : list
        The list to be chunked
    chunk_size : int
        The ideal size of a chunk
    size_func : Callabale
        A function that computes the size of each item

    Returns
    -------
    list
        a list of slices, each represent a chunk
    """

    start = 0
    slices: List[slice] = []
    slice_size = 0
    for index, item in enumerate(list_to_chunk):
        # Create a chunk if there's enough items already
        if index - start >= items_per_chunk:
            slices.append(slice(start, index))
            start = index
            slice_size = 0

        item_size = size_func(item)
        slice_size += item_size
        if slice_size > chunk_size:
            if start == index:
                # Put an item into its own chunk if it exceeds the chunk size
                slices.append(slice(start, index + 1))
                start = index + 1
                slice_size = 0
            else:
                slices.append(slice(start, index))
                start = index
                slice_size = item_size

    # last chunk
    if slice_size > 0:
        slices.append(slice(start, len(list_to_chunk)))

    return slices


def removesuffix(text: str, suffix: str):
    if text.endswith(suffix):
        return text[: -len(suffix)]
    else:
        return text


def filter_none(lst: List) -> List:
    """Filter out all None values from the list"""
    return [e for e in lst if e is not None]


def is_email(email: str) -> bool:
    try:
        validate_email(email)
    except (ValueError, AssertionError):
        return False
    return True


def to_utc_datetime_from_timestamp(
    timestamp: Optional[Union[float, int]] = None,
    tzinfo: Optional[timezone] = timezone.utc,
) -> Optional[datetime]:
    """
    Return a datetime from timestamp, None if timestamp are None
    """
    if timestamp is not None:
        return datetime.fromtimestamp(timestamp, tz=tzinfo).replace(tzinfo=timezone.utc)

    return None
