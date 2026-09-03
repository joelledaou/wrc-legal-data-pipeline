"""Split a date range into calendar partitions.

The scraper runs one search per (partition, body) and stamps every record with
the partition's start date as `partition_date`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

PARTITION_SIZES = ("monthly", "weekly", "daily")


@dataclass(frozen=True)
class Partition:
    label: str  # "2024-01" for months, ISO start date otherwise
    start: date  # inclusive
    end: date  # inclusive, since the site's date filters are


def build_partitions(start_date: date, end_date: date, size: str = "monthly") -> list[Partition]:
    """Partition [start_date, end_date) into chunks, clamped to the range."""
    if end_date <= start_date:
        raise ValueError(f"end_date ({end_date}) must be after start_date ({start_date})")
    if size not in PARTITION_SIZES:
        raise ValueError(f"Unknown partition size '{size}'. Use one of: {', '.join(PARTITION_SIZES)}")

    last_day = end_date - timedelta(days=1)
    partitions = []
    cursor = start_date
    while cursor <= last_day:
        if size == "monthly":
            next_month = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
            chunk_end = next_month - timedelta(days=1)
            label = cursor.strftime("%Y-%m")
        else:
            chunk_end = cursor + timedelta(days=6 if size == "weekly" else 0)
            label = cursor.isoformat()
        chunk_end = min(chunk_end, last_day)
        partitions.append(Partition(label, cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return partitions
