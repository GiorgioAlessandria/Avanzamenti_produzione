from datetime import date, datetime
from types import SimpleNamespace

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


def test_kpi_due_date_reads_json_value_for_closed_phase():
    log = SimpleNamespace(
        DataFineSched='["2026-07-09 00:00:00"]',
        NumFase="[1.0, 2.0]",
        FaseConsuntivata="2",
        FaseAttiva="",
    )

    assert service._kpi_data_fine_prevista(log) == date(2026, 7, 9)


def test_kpi_completed_order_and_machine_require_final_state_and_serial_number():
    runtime = SimpleNamespace(StatoOrdinePost="Chiusa", StatoOdpPost="")
    machine = SimpleNamespace(StatoOrdinePost="Chiusa", GestioneMatricola="si")
    intermediate = SimpleNamespace(StatoOrdinePost="Pianificata", GestioneMatricola="si")
    ordinary = SimpleNamespace(StatoOrdinePost="Chiusa", GestioneMatricola="no")

    assert service._kpi_is_order_completed(runtime, machine) is True
    assert service._kpi_macchine_prodotte(runtime, machine) == 1.0
    assert service._kpi_macchine_prodotte(runtime, intermediate) == 0.0
    assert service._kpi_macchine_prodotte(runtime, ordinary) == 0.0


def test_kpi_time_reliability_requires_positive_times_within_ten_percent():
    assert service._kpi_tempo_is_affidabile(10.0, 9.0) is True
    assert service._kpi_tempo_is_affidabile(10.0, 11.0) is True
    assert service._kpi_tempo_is_affidabile(10.0, 8.9) is False
    assert service._kpi_tempo_is_affidabile(10.0, 11.1) is False
    assert service._kpi_tempo_is_affidabile(10.0, 0.0) is False


def test_kpi_quantities_and_nonconformity_inputs_are_normalized():
    log = SimpleNamespace(
        QuantitaConforme="98,5",
        QuantitaNonConforme="1,5",
    )

    assert service._kpi_quantita_consuntivata(log) == (98.5, 1.5)
    assert service._kpi_quantita_consuntivata(None) == (0.0, 0.0)


def test_kpi_lead_time_uses_first_takeover_timestamp():
    runtime = SimpleNamespace(
        DataInCaricoPre="2026-07-09T08:00:00+02:00",
        DataInCaricoPost="2026-07-09T09:00:00+02:00",
    )
    closed_at = datetime.fromisoformat("2026-07-10T08:30:00+02:00")

    assert service._kpi_lead_time_ore(runtime, closed_at) == 24.5


def test_kpi_data_coverage_requires_time_quantity_and_due_date():
    complete = {
        "tempo_previsto_ore": 2.0,
        "tempo_reale_ore": 2.5,
        "quantita_consuntivata": 10.0,
        "data_fine_prevista": "2026-07-09",
    }

    assert service._kpi_row_has_complete_data(complete) is True
    assert service._kpi_row_has_complete_data(
        {**complete, "quantita_consuntivata": 0}
    ) is False
    assert service._kpi_row_has_complete_data(
        {**complete, "data_fine_prevista": ""}
    ) is False


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
        "fasi_consuntivate": 0,
        "ritardi": 0,
        "giorni_ritardo_totali": 0.0,
        "tempo_previsto": 0.0,
        "tempo_reale": 0.0,
        "scostamento": 0.0,
    }


def test_apply_kpi_group_separates_phases_from_completed_orders():
    bucket = service._new_kpi_group_bucket("R1")
    base = {
        "tempo_previsto_ore": 2.0,
        "tempo_reale_ore": 3.0,
        "scostamento_ore": 1.0,
        "ritardo_giorni": 4,
    }

    service._apply_kpi_group(bucket, {**base, "is_order_completed": False})
    service._apply_kpi_group(bucket, {**base, "is_order_completed": True})

    assert bucket["fasi_consuntivate"] == 2
    assert bucket["ordini"] == 1
    assert bucket["ritardi"] == 1
    assert bucket["giorni_ritardo_totali"] == 4
    assert bucket["tempo_previsto"] == 4.0
    assert bucket["tempo_reale"] == 6.0


def test_finalize_kpi_group_calculates_percentages_and_rounding():
    assert service._finalize_kpi_group(
        {
            "key": "R1",
            "ordini": 4,
            "fasi_consuntivate": 6,
            "ritardi": 2,
            "giorni_ritardo_totali": 3.0,
            "tempo_previsto": 10.0,
            "tempo_reale": 12.345,
            "scostamento": 2.345,
        }
    ) == {
        "key": "R1",
        "ordini": 4,
        "fasi_consuntivate": 6,
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
