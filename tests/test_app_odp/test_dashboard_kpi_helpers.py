from datetime import date, datetime

import pytest

from app_odp.services import dashboard_service as service


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-07-09", date(2026, 7, 9)),
        ("2026-07-09 14:30:00", date(2026, 7, 9)),
        ("09/07/2026", date(2026, 7, 9)),
        ("09/07/2026 14:30:00", date(2026, 7, 9)),
        ("bad", None),
        ("", None),
    ],
)
def test_kpi_parse_date_accepts_supported_formats(value, expected):
    assert service._kpi_parse_date(value) == expected


def test_kpi_parse_datetime_returns_timezone_aware_datetime_or_none():
    parsed = service._kpi_parse_datetime("2026-07-09 14:30:00")

    assert isinstance(parsed, datetime)
    assert parsed.date() == date(2026, 7, 9)
    assert parsed.tzinfo is not None
    assert service._kpi_parse_datetime("bad") is None
    assert service._kpi_parse_datetime("") is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", []),
        (None, []),
        ('["A", "", "B"]', ["A", "B"]),
        ('"A"', ["A"]),
        ("raw", ["raw"]),
    ],
)
def test_kpi_jsonish_list_normalizes_json_or_raw_values(value, expected):
    assert service._kpi_jsonish_list(value) == expected


def test_kpi_active_value_from_list_prefers_matching_phase_then_index():
    assert service._kpi_active_value_from_list('["R1", "R2"]', '["10", "20"]', "20") == "R2"
    assert service._kpi_active_value_from_list('["R1", "R2"]', "", "2") == "R2"
    assert service._kpi_active_value_from_list('["R1"]', "", "99") == "R1"
    assert service._kpi_active_value_from_list("", "", "1") == ""


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("3", 3),
        (None, 0),
        ("bad", 0),
    ],
)
def test_snapshot_int_returns_int_or_zero(value, expected):
    assert service._snapshot_int(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("3.5", 3.5),
        (None, 0.0),
        ("bad", 0.0),
    ],
)
def test_snapshot_float_returns_float_or_zero(value, expected):
    assert service._snapshot_float(value) == expected


def test_snapshot_change_calculates_delta_and_percent():
    assert service._snapshot_change("12.5", "10") == {
        "current": 12.5,
        "previous": 10.0,
        "delta": 2.5,
        "delta_percent": 25.0,
    }

    assert service._snapshot_change(5, 0) == {
        "current": 5.0,
        "previous": 0.0,
        "delta": 5.0,
        "delta_percent": 0.0,
    }


def test_new_kpi_group_bucket_uses_zero_defaults():
    assert service._new_kpi_group_bucket("R1") == {
        "key": "R1",
        "ordini": 0,
        "ritardi": 0,
        "giorni_ritardo_totali": 0.0,
        "tempo_previsto": 0.0,
        "tempo_reale": 0.0,
        "scostamento": 0.0,
    }


def test_finalize_kpi_group_calculates_percentages_and_rounding():
    assert service._finalize_kpi_group(
        {
            "key": "R1",
            "ordini": 4,
            "ritardi": 2,
            "giorni_ritardo_totali": 3.0,
            "tempo_previsto": 10.0,
            "tempo_reale": 12.345,
            "scostamento": 2.345,
        }
    ) == {
        "key": "R1",
        "ordini": 4,
        "ritardi": 2,
        "percentuale_ritardo": 50.0,
        "giorni_medi_ritardo": 1.5,
        "tempo_previsto": 10.0,
        "tempo_reale": 12.35,
        "scostamento": 2.35,
        "scostamento_percentuale": 23.45,
    }


def test_excel_safe_serializes_none_and_booleans():
    assert service._excel_safe(None) == ""
    assert service._excel_safe(True) == "S\u00ec"
    assert service._excel_safe(False) == "No"
    assert service._excel_safe("test") == "test"
