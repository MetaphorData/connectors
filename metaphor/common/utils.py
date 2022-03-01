from datetime import datetime, time, timedelta, timezone


def start_of_day(daysAgo=0) -> datetime:
    """return the start of day in UTC time, for today or N days ago"""
    return datetime.combine(
        datetime.now().date(), time(tzinfo=timezone.utc)
    ) - timedelta(days=daysAgo)
