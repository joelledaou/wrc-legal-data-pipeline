from datetime import date

import pytest

from wrc_pipeline.partitions import build_partitions


def test_monthly_partitions_are_clamped_to_the_range():
    partitions = build_partitions(date(2024, 1, 15), date(2024, 3, 1))

    assert [(p.label, p.start, p.end) for p in partitions] == [
        ("2024-01", date(2024, 1, 15), date(2024, 1, 31)),
        ("2024-02", date(2024, 2, 1), date(2024, 2, 29)),
    ]


def test_weekly_and_daily_partitions_cover_every_day_once():
    weekly = build_partitions(date(2024, 1, 1), date(2024, 1, 16), "weekly")
    daily = build_partitions(date(2024, 1, 1), date(2024, 1, 4), "daily")

    assert [(p.start, p.end) for p in weekly] == [
        (date(2024, 1, 1), date(2024, 1, 7)),
        (date(2024, 1, 8), date(2024, 1, 14)),
        (date(2024, 1, 15), date(2024, 1, 15)),
    ]
    assert [p.label for p in daily] == ["2024-01-01", "2024-01-02", "2024-01-03"]


def test_invalid_ranges_and_sizes_are_rejected():
    with pytest.raises(ValueError):
        build_partitions(date(2024, 2, 1), date(2024, 1, 1))
    with pytest.raises(ValueError):
        build_partitions(date(2024, 1, 1), date(2024, 2, 1), "yearly")
