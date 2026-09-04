from datetime import date

import pytest

from wrc_pipeline.partitions import build_partitions


def test_monthly_partitions_are_clamped_to_the_range_but_keep_the_calendar_start():
    partitions = build_partitions(date(2024, 1, 15), date(2024, 3, 1))

    assert [(p.label, p.period_start, p.start, p.end) for p in partitions] == [
        ("2024-01", date(2024, 1, 1), date(2024, 1, 15), date(2024, 1, 31)),
        ("2024-02", date(2024, 2, 1), date(2024, 2, 1), date(2024, 2, 29)),
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


def test_weekly_partitions_are_iso_weeks_whatever_day_the_range_starts_on():
    partitions = build_partitions(date(2024, 1, 17), date(2024, 1, 24), "weekly")  # Wednesday to Tuesday

    assert [(p.label, p.period_start, p.start, p.end) for p in partitions] == [
        ("2024-01-15", date(2024, 1, 15), date(2024, 1, 17), date(2024, 1, 21)),
        ("2024-01-22", date(2024, 1, 22), date(2024, 1, 22), date(2024, 1, 23)),
    ]


def test_invalid_ranges_and_sizes_are_rejected():
    with pytest.raises(ValueError):
        build_partitions(date(2024, 2, 1), date(2024, 1, 1))
    with pytest.raises(ValueError):
        build_partitions(date(2024, 1, 1), date(2024, 2, 1), "yearly")
