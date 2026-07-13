from datetime import datetime, time
from zoneinfo import ZoneInfo

import pytest

from sync import sync_acq


def test_sync_acq_text_and_lookup_normalizers_clear_empty_values():
    assert sync_acq._norm_text("  abc  ") == "abc"
    assert sync_acq._norm_text(None) == ""
    assert sync_acq._norm_text(float("nan")) == ""

    assert sync_acq._normalize_variante_lookup(" X ") == ""
    assert sync_acq._normalize_variante_lookup("-") == ""
    assert sync_acq._normalize_variante_lookup("V1") == "V1"

    assert sync_acq._normalize_indice_lookup(" none ") == ""
    assert sync_acq._normalize_indice_lookup("A") == "A"


def test_sync_acq_safe_number_helpers_parse_erp_style_values():
    assert sync_acq._safe_float("1.234,5") == 1234.5
    assert sync_acq._safe_float(None, default=7.0) == 7.0
    assert sync_acq._safe_float(float("nan"), default=2.0) == 2.0
    assert sync_acq._safe_int("2,6") == 3


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, []),
        ("", []),
        ("not-json", []),
        ('{"CodArt": "A"}', []),
        ([{"CodArt": "A"}], [{"CodArt": "A"}]),
        ('[{"CodArt": "A"}]', [{"CodArt": "A"}]),
    ],
)
def test_parse_distinta_materiale_returns_only_lists(value, expected):
    assert sync_acq._parse_distinta_materiale(value) == expected


def test_sync_acq_in_time_window_handles_day_and_overnight_windows():
    assert sync_acq._in_time_window(time(8, 0), time(8, 0), time(17, 0)) is True
    assert sync_acq._in_time_window(time(17, 0), time(8, 0), time(17, 0)) is False
    assert sync_acq._in_time_window(time(23, 0), time(22, 0), time(6, 0)) is True
    assert sync_acq._in_time_window(time(6, 0), time(22, 0), time(6, 0)) is False
    assert sync_acq._in_time_window(time(12, 0), time(8, 0), time(8, 0)) is True


def test_sync_acq_is_allowed_datetime_uses_start_day_for_overnight_windows():
    monday_late = datetime(2026, 7, 6, 23, 0)
    tuesday_early = datetime(2026, 7, 7, 5, 0)
    tuesday_late = datetime(2026, 7, 7, 23, 0)

    assert sync_acq._is_allowed_datetime(monday_late, time(22, 0), time(6, 0), {0}) is True
    assert sync_acq._is_allowed_datetime(tuesday_early, time(22, 0), time(6, 0), {0}) is True
    assert sync_acq._is_allowed_datetime(tuesday_late, time(22, 0), time(6, 0), {0}) is False


def test_seconds_until_next_allowed_returns_zero_inside_window(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 7, 9, 8, 30, tzinfo=tz)

    monkeypatch.setattr(sync_acq, "datetime", FixedDatetime)

    assert sync_acq.seconds_until_next_allowed(8, 17, {3}, tz=ZoneInfo("Europe/Rome")) == 0


def test_seconds_until_next_allowed_returns_seconds_until_next_probe(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 7, 9, 7, 59, tzinfo=tz)

    monkeypatch.setattr(sync_acq, "datetime", FixedDatetime)

    assert sync_acq.seconds_until_next_allowed(8, 17, {3}, tz=ZoneInfo("Europe/Rome")) == 60
