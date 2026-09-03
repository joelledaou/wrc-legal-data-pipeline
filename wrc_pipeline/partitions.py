"""Split a [start_date, end_date) range into calendar partitions.

The scraper iterates over these partitions (one search per partition per body)
and stamps every record with the partition's start date as `partition_date`.
E.g. monthly partitions between 2024-01-01 and 2025-01-01 produce the twelve
months of 2024.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class Partition:
    label: str        # human-readable, e.g. "2024-01"
    start: date       # first day covered (inclusive) — stored as partition_date
    end: date         # last day covered (inclusive) — the site's date filters are inclusive


def build_partitions(start_date: date, end_date: date, size: str = "monthly") -> list[Partition]:
    """Partition [start_date, end_date) into monthly, weekly, or daily chunks.

    end_date is exclusive, matching the convention "between 01-01-2024 and
    01-01-2025 means the year 2024". Chunks at the edges are clamped to the
    requested range.
    """
    if end_date <= start_date:
        raise ValueError(f"end_date ({end_date}) must be after start_date ({start_date})")

    last_day = end_date - timedelta(days=1)
    partitions: list[Partition] = []
    cursor = start_date

    if size == "monthly":
        while cursor <= last_day:
            next_month = date(cursor.year + 1, 1, 1) if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
            month_end = next_month - timedelta(days=1)
            chunk_end = min(month_end, last_day)
            partitions.append(Partition(cursor.strftime("%Y-%m"), cursor, chunk_end))
            cursor = chunk_end + timedelta(days=1)
    elif size in ("weekly", "daily"):
        step = 7 if size == "weekly" else 1
        while cursor <= last_day:
            chunk_end = min(cursor + timedelta(days=step - 1), last_day)
            partitions.append(Partition(cursor.isoformat(), cursor, chunk_end))
            cursor = chunk_end + timedelta(days=1)
    else:
        raise ValueError(f"Unknown partition size '{size}'. Use monthly, weekly, or daily.")

    return partitions
