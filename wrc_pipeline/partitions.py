"""Split a date range into calendar partitions.

The scraper runs one search per (partition, body) and stamps every record with
the calendar start of its period as `partition_date`. Partitions are calendar
months, ISO weeks or days, so a record lands in the same partition whatever
range the run was started with; only the searched window is clamped to the
requested range.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

PARTITION_SIZES = ("monthly", "weekly", "daily")


@dataclass(frozen=True)
class Partition:
    label: str  # "2024-01" for months, ISO period start otherwise
    period_start: date  # calendar start of the period, stamped on records as partition_date
    start: date  # first day searched: the period start, clamped to the requested range
    end: date  # last day searched, inclusive, since the site's date filters are


def build_partitions(start_date: date, end_date: date, size: str = "monthly") -> list[Partition]:
    """Partition [start_date, end_date) into calendar periods, clamped to the range."""
    if end_date <= start_date:
        raise ValueError(f"end_date ({end_date}) must be after start_date ({start_date})")
    if size not in PARTITION_SIZES:
        raise ValueError(f"Unknown partition size '{size}'. Use one of: {', '.join(PARTITION_SIZES)}")

    last_day = end_date - timedelta(days=1)
    partitions = []
    cursor = start_date
    while cursor <= last_day:
        period_start, period_end = calendar_period(cursor, size)
        label = period_start.strftime("%Y-%m") if size == "monthly" else period_start.isoformat()
        partitions.append(Partition(label, period_start, cursor, min(period_end, last_day)))
        cursor = period_end + timedelta(days=1)
    return partitions


def calendar_period(day: date, size: str) -> tuple[date, date]:
    """First and last day of the calendar month, ISO week or day containing `day`."""
    if size == "monthly":
        first = day.replace(day=1)
        return first, (first + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    if size == "weekly":
        first = day - timedelta(days=day.weekday())
        return first, first + timedelta(days=6)
    return day, day
