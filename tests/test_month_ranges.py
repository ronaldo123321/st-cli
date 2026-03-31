"""Unit tests for date helpers."""

from datetime import date

from st_cli.st_api import month_ranges_last_n_months


def test_month_ranges_count_and_order():
    ranges = month_ranges_last_n_months(36, end=date(2026, 3, 15))
    assert len(ranges) == 36
    # Newest first: March 2026 then February 2026
    assert ranges[0][0] == date(2026, 3, 1)
    assert ranges[1][0] == date(2026, 2, 1)
