from datetime import datetime, time, timedelta, timezone


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
