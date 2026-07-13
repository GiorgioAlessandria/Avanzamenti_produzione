from datetime import date

from sync.sync_acq import add_workdays


def test_add_workdays_returns_same_date_for_zero_or_negative_days():
    start = date(2026, 7, 9)

    assert add_workdays(start, 0) == start
    assert add_workdays(start, -2) == start


def test_add_workdays_skips_weekend():
    friday = date(2026, 7, 10)

    assert add_workdays(friday, 1) == date(2026, 7, 13)
    assert add_workdays(friday, 3) == date(2026, 7, 15)

