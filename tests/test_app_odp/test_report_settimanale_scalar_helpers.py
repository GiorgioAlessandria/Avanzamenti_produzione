from datetime import datetime
from types import SimpleNamespace

import pytest

from app_odp.services import report_settimanale_service as service


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2,5", 2.5),
        ("3.25", 3.25),
        ("", 0.0),
        (None, 0.0),
        ("bad", 0.0),
    ],
)
def test_to_float_parses_numbers_or_returns_zero(value, expected):
    assert service._to_float(value) == expected


def test_parse_iso_and_format_dt_handle_valid_and_invalid_values():
    parsed = service._parse_iso("2026-07-09T14:30:00")

    assert parsed == datetime(2026, 7, 9, 14, 30)
    assert service._parse_iso("bad") is None
    assert service._format_dt("2026-07-09T14:30:00") == "09/07/2026 14:30"
    assert service._format_dt("bad") == "bad"


def test_num_progr_riga_prefers_current_then_post_then_pre_then_dash():
    assert service._num_progr_riga(SimpleNamespace(NumProgrRiga=" 5 ")) == "5"
    assert service._num_progr_riga(SimpleNamespace(NumProgrRiga="", NumProgrRigaPost="6")) == "6"
    assert service._num_progr_riga(SimpleNamespace(NumProgrRiga="", NumProgrRigaPost="", NumProgrRigaPre="7")) == "7"
    assert service._num_progr_riga(SimpleNamespace()) == "-"


def test_phase_value_normalizes_json_numbers_and_fallbacks():
    assert service._phase_value(SimpleNamespace(NumFase="[1.0]")) == "1"
    assert service._phase_value(SimpleNamespace(FaseAttiva="2")) == "2"
    assert service._phase_value(SimpleNamespace(Fase="collaudo")) == "collaudo"
    assert service._phase_value(SimpleNamespace()) == "0"


def test_report_row_keys_use_order_phase_and_progressive_row():
    row = SimpleNamespace(IdDocumento=" DOC ", IdRiga=" 10 ", NumFase="[2]", NumProgrRiga="3")

    assert service._order_key(row) == ("DOC", "10")
    assert service._phase_key(row) == ("DOC", "10", "2")
    assert service._order_key_from_phase_key(("DOC", "10", "2")) == ("DOC", "10")
    assert service._work_row_key(row) == ("DOC", "10", "2", "3")


def test_log_state_helpers_detect_deleted_activation_stop_and_closed_states():
    assert service._is_deleted_state("Eliminato dal gestionale", "") is True
    assert service._is_deleted_state("", "") is False
    assert service._is_activation_input_log(SimpleNamespace(StatoOrdinePost="Attiva")) is True
    assert service._is_stop_input_log(SimpleNamespace(StatoOrdinePost="Sospesa")) is True
    assert service._is_closed_log(SimpleNamespace(StatoOrdinePost="", StatoOdpPost="Chiusa")) is True
    assert service._is_closed_log(SimpleNamespace(StatoOrdinePost="Attiva", StatoOdpPost="")) is False


def test_runtime_delta_hours_returns_positive_delta_only():
    assert service._runtime_delta_hours(
        SimpleNamespace(
            ElapsedSeconds="7200",
            TempoNonFunzionamentoSecondi="3600",
        )
    ) == 1.0
    assert service._runtime_delta_hours(
        SimpleNamespace(TempoFunzionamentoPre="1,5", TempoFunzionamentoPost="2.75")
    ) == 1.25
    assert service._runtime_delta_hours(
        SimpleNamespace(
            ElapsedSeconds="0",
            TempoFunzionamentoPre="1",
            TempoFunzionamentoPost="8",
        )
    ) == 0.0
    assert service._runtime_delta_hours(
        SimpleNamespace(TempoFunzionamentoPre="3", TempoFunzionamentoPost="2")
    ) == 0.0
    assert service._runtime_delta_hours(None) == 0.0


def test_runtime_map_links_group_stop_to_actual_elapsed_event():
    actual = SimpleNamespace(
        OperationGroupId="runtime",
        IdDocumento="DOC",
        IdRiga="1",
        EventAt="2026-07-24T07:09:55+02:00",
        ElapsedSeconds="1800",
        TempoNonFunzionamentoSecondi="",
    )
    stop = SimpleNamespace(
        OperationGroupId="stop",
        IdDocumento="DOC",
        IdRiga="1",
        EventAt="2026-07-24T07:09:55+02:00",
        ElapsedSeconds="0",
        TempoFunzionamentoPre="",
        TempoFunzionamentoPost="12",
    )

    assert service._runtime_by_operation_group([actual, stop])["stop"] is actual


def test_final_close_and_runtime_current_hours_read_scalar_fields():
    assert service._final_close_hours(SimpleNamespace(TempoFunzionamentoFinale="4,5")) == 4.5
    assert service._runtime_current_hours(SimpleNamespace(Tempo_funzionamento="1,25")) == 1.25
    assert service._runtime_current_hours(SimpleNamespace(TempoFunzionamento="2")) == 2.0
    assert service._runtime_current_hours(None) == 0.0


def test_quantity_helpers_parse_machine_and_produced_quantities():
    assert service._is_macchina(SimpleNamespace(GestioneMatricola="SI")) is True
    assert service._is_macchina(SimpleNamespace(GestioneMatricola="no")) is False
    assert service._qta_finale(SimpleNamespace(Quantita="5,5")) == 5.5
    assert service._qta_prodotta(
        SimpleNamespace(QuantitaConforme="3", QuantitaNonConforme="1,5")
    ) == 4.5
    assert service._qta_prodotta(SimpleNamespace(QuantitaConforme="", QuantitaNonConforme="")) == 0.0


def test_label_user_state_and_chunk_helpers():
    assert service._row_label(SimpleNamespace(RifRegistraz="RIF", IdDocumento="DOC", IdRiga="1")) == "RIF"
    assert service._row_label(SimpleNamespace(RifRegistraz="", IdDocumento="DOC", IdRiga="1")) == "DOC.1"
    assert service._username(SimpleNamespace(username="mario")) == "mario"
    assert service._username(None) == ""
    assert service._latest_state(SimpleNamespace(StatoOdpPost="Attiva", StatoOrdinePost="Pianificata")) == "Attiva"
    assert service._latest_state(SimpleNamespace(StatoOdpPost="", StatoOrdinePost="Pianificata")) == "Pianificata"
    assert service._latest_state(SimpleNamespace()) == "-"
    assert list(service._chunks([("a", "1"), ("b", "2"), ("c", "3")], size=2)) == [
        [("a", "1"), ("b", "2")],
        [("c", "3")],
    ]
