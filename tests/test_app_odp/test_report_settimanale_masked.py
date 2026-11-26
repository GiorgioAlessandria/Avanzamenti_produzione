import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app_odp.services import report_settimanale_service as service


START = datetime.fromisoformat('2026-11-23T00:00:00+01:00')
END = datetime.fromisoformat('2026-11-29T23:59:59+01:00')


def log(document, state, hour, operation, final=''):
    return SimpleNamespace(
        IdDocumento=document, IdRiga='1', NumFase='1', OperationGroupId=operation,
        ClosedBy='Mario', ClosedAt=f'2026-11-24T{hour}:00:00+01:00',
        StatoOrdinePost=state, StatoOrdinePre='Attivo',
        TempoFunzionamentoFinale=final, GestioneMatricola='no', Quantita='3',
        CodArt='ART', DesArt='Articolo', RifRegistraz=document,
    )


def calculate(rows, events):
    masked = {}
    result = service._calculate_worked_hours_by_user_phase(
        input_logs=rows, runtime_map={}, start_dt=START, end_dt=END,
        masked_hours=masked, runtime_events=events,
    )
    return result, masked


def test_masked_closures_keep_theoretical_hours_without_counting_elapsed():
    rows = [log('MASK', 'Attivo', '08', 'a'), log('MASK', 'In Sospeso', '12', 'p', '0.5'),
            log('MASK', 'Chiusa', '13', 'f', '0.25')]
    events = {key: SimpleNamespace(PayloadJson=json.dumps({'member_role': 'MASKED'})) for key in ('p', 'f')}
    result, masked = calculate(rows, events)
    assert result == masked == {'Mario': {('MASK', '1', '1'): 0.75}}


def test_same_phase_can_contain_both_normal_and_masked_hours():
    rows = [log('DOC', 'Attivo', '08', 'a'), log('DOC', 'In Sospeso', '10', 's'),
            log('DOC', 'Attivo', '10', 'b'), log('DOC', 'Chiusa', '14', 'f', '0.5')]
    result, masked = calculate(rows, {'f': SimpleNamespace(PayloadJson='{"member_role":"MASKED"}')})
    assert result['Mario'][('DOC', '1', '1')] == 2.5
    assert masked['Mario'][('DOC', '1', '1')] == 0.5


def test_masked_closure_outside_period_is_not_added():
    row = log('DOC', 'Chiusa', '12', 'f', '0.5')
    row.ClosedAt = '2026-11-20T12:00:00+01:00'
    assert calculate([row], {'f': SimpleNamespace(PayloadJson='{"member_role":"MASKED"}')}) == ({}, {})


def test_membership_fallback_respects_phase_dates_and_explicit_normal_role():
    row = log('DOC', 'Attivo', '08', 'a')
    member = SimpleNamespace(IdDocumento='DOC', IdRiga='1', Fase='1', Role='MASKED',
                             CreatedAt=START.isoformat(), ClosedAt='', DissolvedAt='')
    assert service._is_masked_work(row, None, [member])
    assert not service._is_masked_work(row, SimpleNamespace(PayloadJson='{"member_role":"MAIN"}'), [member])
    member.Fase = '2'
    assert not service._is_masked_work(row, None, [member])
    member.Fase = '1'
    member.DissolvedAt = '2026-11-24T07:00:00+01:00'
    assert not service._is_masked_work(row, None, [member])


def test_assigned_zero_runtime_is_masked_even_if_state_event_has_no_role():
    row = log('DOC', 'In Sospeso', '12', 's')
    assert service._is_masked_work(
        row, SimpleNamespace(PayloadJson='{"group_uid":"MASK-1"}'), [],
        SimpleNamespace(PayloadJson='{"time_share_mode":"ZERO"}'),
    )


def test_open_masked_order_is_visible_and_its_hours_are_marked_for_exclusion():
    row = log('DOC', 'Attivo', '08', 'a')
    member = SimpleNamespace(IdDocumento='DOC', IdRiga='1', Fase='1', Role='MASKED',
                             CreatedAt=START.isoformat(), ClosedAt='', DissolvedAt='')
    masked = {}
    result = service._calculate_worked_hours_by_user_phase(
        input_logs=[row], runtime_map={}, start_dt=START,
        end_dt=datetime.fromisoformat('2026-11-24T12:00:00+01:00'),
        masked_hours=masked, masked_members=[member],
    )
    assert result == masked == {'Mario': {('DOC', '1', '1'): 4.0}}


@pytest.mark.parametrize('only_masked', [False, True])
def test_report_retains_rows_and_excludes_masked_hours_from_kpi(monkeypatch, only_masked):
    rows = [log('MASK', 'Attivo', '08', 'm-a'), log('MASK', 'Chiusa', '12', 'm-f', '0.5')]
    if not only_masked:
        rows += [log('NORMAL', 'Attivo', '08', 'n-a'), log('NORMAL', 'Chiusa', '12', 'n-f')]
    event = SimpleNamespace(IdDocumento='MASK', IdRiga='1', OperationGroupId='m-f', PayloadJson='{"member_role":"MASKED"}')
    monkeypatch.setattr(service, '_load_user_by_id', lambda user_id: SimpleNamespace(id=1, username='Mario'))
    monkeypatch.setattr(service, '_load_deleted_order_keys', lambda: set())
    monkeypatch.setattr(service, '_load_runtime_logs_in_period', lambda **kwargs: [event])
    monkeypatch.setattr(service, '_load_input_logs_in_period_for_user', lambda **kwargs: rows)
    monkeypatch.setattr(service, '_load_active_runtime_order_keys_for_user', lambda **kwargs: set())
    monkeypatch.setattr(service, '_load_input_logs_for_orders', lambda keys: rows)
    monkeypatch.setattr(service, '_runtime_by_operation_group', lambda logs: {})
    monkeypatch.setattr(service, '_weekly_capacity_hours', lambda user_id: 40)
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    monkeypatch.setattr(service, 'db', SimpleNamespace(session=session))
    report = service.build_report_settimanale_for_user(
        selected_user_id=1, start_dt=START, end_dt=END, can_select_user=True, users=[],
    )
    assert report['kpi']['ore_impiegate'] == (0 if only_masked else 4)
    assert report['kpi']['coefficiente_impiego'] == (0 if only_masked else 10)
    masked_row = next(row for row in report['ordini_lavorati'] if row['id_documento'] == 'MASK')
    assert masked_row['ore_impiegate'] == masked_row['ore_mascherate'] == 0.5
    assert len(report['ordini_lavorati']) == (1 if only_masked else 2)
