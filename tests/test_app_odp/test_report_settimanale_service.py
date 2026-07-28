from types import SimpleNamespace

from app_odp.services import report_settimanale_service as service
from app_odp.services.report_settimanale_service import _percent


def test_percent_returns_none_when_planned_is_zero():
    assert _percent(10, 0) is None


def test_percent_returns_rounded_deviation():
    assert _percent(3, 2) == 50.0
    assert _percent(1, 3) == -66.67
    assert _percent(2, 2) == 0.0


def test_employment_coefficient_relates_worked_hours_to_capacity():
    assert service._employment_coefficient(20, 40) == 50.0
    assert service._employment_coefficient(40, 40) == 100.0
    assert service._employment_coefficient(50, 40) == 125.0
    assert service._employment_coefficient(10, 0) is None


def test_worked_hours_with_fallback_uses_confirmed_source_precedence():
    input_log = SimpleNamespace(
        StatoOrdinePost="Chiuso",
        StatoOdpPost="",
        TempoFunzionamentoFinale="9",
        OperationGroupId="op-1",
        IdDocumento="DOC1",
        IdRiga="2",
        NumFase="3",
        RifRegistraz="RIF-1",
    )
    runtime = SimpleNamespace(
        TempoFunzionamentoPre="1",
        TempoFunzionamentoPost="8",
    )
    phase_map = {("DOC1", "2", "3"): SimpleNamespace(Tempo_funzionamento="5")}
    order_map = {("DOC1", "2"): SimpleNamespace(Tempo_funzionamento="3")}
    rif_map = {"RIF-1": SimpleNamespace(Tempo_funzionamento="1")}
    sources = {
        "runtime_map": {"op-1": runtime},
        "runtime_current_map": phase_map,
        "runtime_current_by_order": order_map,
        "runtime_current_by_rif": rif_map,
    }

    assert service._worked_hours_with_fallback(input_log=input_log, **sources) == 7.0
    runtime.TempoFunzionamentoPost = "1"
    assert service._worked_hours_with_fallback(input_log=input_log, **sources) == 0.0
    sources["runtime_map"].clear()
    assert service._worked_hours_with_fallback(input_log=input_log, **sources) == 9.0
    input_log.StatoOrdinePost = "Attivo"
    assert service._worked_hours_with_fallback(input_log=input_log, **sources) == 5.0
    phase_map.clear()
    assert service._worked_hours_with_fallback(input_log=input_log, **sources) == 3.0
    order_map.clear()

