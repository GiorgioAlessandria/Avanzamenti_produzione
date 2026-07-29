from datetime import datetime
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


def test_active_order_started_before_period_is_a_report_candidate():
    assert service._candidate_order_keys(
        runtime_logs=[],
        input_logs=[],
        active_runtime_order_keys={("DOC", "1")},
    ) == {("DOC", "1")}

    active_log = SimpleNamespace(
        IdDocumento="DOC",
        IdRiga="1",
        NumFase="1",
        OperationGroupId="activation",
        ClosedBy="Mario",
        ClosedAt="2026-07-20T08:00:00+02:00",
        StatoOrdinePost="Attivo",
    )
    result = service._calculate_worked_hours_by_user_phase(
        input_logs=[active_log],
        runtime_map={},
        start_dt=datetime.fromisoformat("2026-07-21T12:00:00+02:00"),
        end_dt=datetime.fromisoformat("2026-07-28T12:00:00+02:00"),
    )

    assert result == {"Mario": {("DOC", "1", "1"): 168.0}}


def test_group_closure_uses_latest_assigned_runtime_even_in_a_later_second():
    def runtime(operation_id, action, event_at, elapsed, active_from=""):
        return SimpleNamespace(
            OperationGroupId=operation_id,
            IdDocumento="DOC",
            IdRiga="1",
            Azione=action,
            EventAt=event_at,
            ElapsedSeconds=str(elapsed),
            TempoNonFunzionamentoSecondi="",
            DataUltimaAttivazionePre=active_from,
        )

    previous = runtime(
        "runtime-previous",
        "runtime_gruppo",
        "2026-07-28T09:00:00+02:00",
        600,
    )
    assigned = runtime(
        "runtime-current",
        "runtime_gruppo",
        "2026-07-28T10:00:00+02:00",
        1800,
    )
    closure = runtime(
        "closure",
        "chiusura_finale",
        "2026-07-28T10:00:02+02:00",
        0,
    )

    runtime_map = service._runtime_by_operation_group(
        [previous, assigned, closure]
    )

    assert runtime_map["closure"] is assigned
    assert service._runtime_delta_hours(runtime_map["closure"]) == 0.5

    normal_closure = runtime(
        "normal-closure",
        "chiusura_finale",
        "2026-07-28T10:00:03+02:00",
        0,
        active_from="2026-07-28T10:00:03+02:00",
    )

    runtime_map = service._runtime_by_operation_group(
        [previous, assigned, normal_closure]
    )

    assert runtime_map["normal-closure"] is normal_closure


def test_weekly_hours_do_not_repeat_cumulative_runtime_for_each_suspension():
    def log(at, state, operation_group):
        return SimpleNamespace(
            IdDocumento="47235",
            IdRiga="1",
            NumFase="[1.0]",
            OperationGroupId=operation_group,
            ClosedBy="Bosio Andrea",
            ClosedAt=at,
            StatoOrdinePost=state,
            TempoNonFunzionamentoSecondi="",
            TempoNonFunzionamentoMinuti="",
            Tempo_funzionamento="41.24",
        )

    rows = []

    for day in range(1, 17):
        rows.extend(
            [
                log(
                    f"2026-07-{day:02d}T08:00:00+02:00",
                    "Attivo",
                    f"activation-{day}",
                ),
                log(
                    f"2026-07-{day:02d}T08:12:00+02:00",
                    "In Sospeso",
                    f"stop-{day}",
                ),
            ]
        )

    rows.extend(
        [
            log(
                "2026-07-28T11:46:00+02:00",
                "Attivo",
                "activation-17",
            ),
            log(
                "2026-07-28T11:58:00+02:00",
                "In Sospeso",
                "stop-17",
            ),
        ]
    )

    result = service._calculate_worked_hours_by_user_phase(
        input_logs=rows,
        runtime_map={},
        start_dt=datetime.fromisoformat("2026-07-21T12:00:00+02:00"),
        end_dt=datetime.fromisoformat("2026-07-28T12:00:00+02:00"),
    )

    assert result == {
        "Bosio Andrea": {
            ("47235", "1", "1"): 0.2,
        }
    }
