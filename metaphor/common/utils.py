from datetime import datetime, time, timedelta, timezone


def start_of_day(daysAgo=0) -> datetime:
    """return the start of day N days ago"""
    return datetime.combine(
        datetime.now().date(), time(tzinfo=timezone.utc)
    ) - timedelta(days=daysAgo)
