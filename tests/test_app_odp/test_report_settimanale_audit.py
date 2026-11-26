from datetime import datetime
from types import SimpleNamespace as Row
from unittest.mock import MagicMock
import json

import pytest

from app_odp.services import report_settimanale_service as service


def dt(text):
    return datetime.fromisoformat(text)


def event(at, state, operation, phase='1', **values):
    return Row(IdDocumento='DOC', IdRiga='1', NumFase='[1,2]', FaseAttiva=phase,
               OperationGroupId=operation, ClosedBy='Mario', ClosedAt=at,
               StatoOrdinePost=state, **values)


def test_multiphase_closure_is_a_stop_and_uses_the_consuntivated_phase():
    start = event('2026-11-24T08:00:00+01:00', 'Attivo', 'a')
    stop = event('2026-11-24T10:00:00+01:00', 'Pianificata', 's', phase='2', FaseConsuntivata='1')
    result = service._calculate_worked_hours_by_user_phase(
        input_logs=[start, stop], runtime_map={},
        start_dt=dt('2026-11-24T00:00:00+01:00'), end_dt=dt('2026-11-24T12:00:00+01:00'),
    )
    assert result == {'Mario': {('DOC', '1', '1'): 2}}
    assert service._phase_value(event('', 'Attivo', 'a', phase='2')) == '2'
    assert service._phase_value(Row(NumFase='3', FaseAttiva='2', FaseConsuntivata='1')) == '1'


def test_clipped_interval_allocates_net_hours_proportionally():
    start = event('2026-11-23T08:00:00+01:00', 'Attivo', 'a')
    stop = event('2026-11-23T16:00:00+01:00', 'In Sospeso', 's')
    assert service._input_interval_hours(
        start, stop, start_dt=dt('2026-11-23T12:00:00+01:00'),
        end_dt=dt('2026-11-24T00:00:00+01:00'), actual_hours=4,
    ) == 2


def test_mixed_legacy_dates_and_daylight_saving_are_supported():
    start = event('2026-10-25T02:30:00+02:00', 'Attivo', 'a')
    stop = event('2026-10-25T02:30:00+01:00', 'In Sospeso', 's')
    assert service._input_interval_hours(start, stop, start_dt=dt('2026-10-25T00:00:00'),
                                         end_dt=dt('2026-10-26T00:00:00')) == 1


def test_suspended_order_closure_does_not_reuse_previous_group_runtime():
    logs = [event('2026-11-24T08:00:00+01:00', 'Attivo', 'a'),
            event('2026-11-24T10:00:00+01:00', 'In Sospeso', 's'),
            event('2026-11-24T12:00:00+01:00', 'Chiusa', 'c', FaseConsuntivata='1')]
    runtime = Row(ElapsedSeconds='7200')
    result = service._calculate_worked_hours_by_user_phase(
        input_logs=logs, runtime_map={'s': runtime, 'c': runtime},
        start_dt=dt('2026-11-24T00:00:00+01:00'), end_dt=dt('2026-11-24T13:00:00+01:00'),
    )
    assert result == {'Mario': {('DOC', '1', '1'): 2}}


def test_multiple_group_with_mul_export_resource_counts_real_time_once():
    rows = [Row(ElapsedSeconds='14400', EventAt='2026-11-24T12:00:00+01:00',
                PayloadJson=json.dumps({'group_uid': 'MULT-1', 'time_share_mode': 'SPLIT',
                                        'elapsed_lavorato_gruppo_seconds': 14400})) for _ in range(2)]
    assigned = service._group_report_hours(rows + rows)
    assert sum(assigned.values()) == 4
    assert list(assigned.values()) == [2, 2]


def test_closed_multiple_group_applies_real_time_shares_to_operator_totals():
    logs, runtime_map = [], {}
    for document in ('ONE', 'TWO'):
        active = event('2026-11-24T08:00:00+01:00', 'Attivo', document + '-a')
        stop = event('2026-11-24T12:00:00+01:00', 'In Sospeso', document + '-s')
        active.IdDocumento = stop.IdDocumento = document
        logs.extend([active, stop])
        runtime_map[stop.OperationGroupId] = Row(
            ElapsedSeconds='14400', EventAt=stop.ClosedAt,
            PayloadJson=json.dumps({'group_uid': 'MULT-1', 'time_share_mode': 'SPLIT',
                                    'elapsed_lavorato_gruppo_seconds': 14400}),
        )
    result = service._calculate_worked_hours_by_user_phase(
        input_logs=logs, runtime_map=runtime_map,
        start_dt=dt('2026-11-24T00:00:00+01:00'), end_dt=dt('2026-11-24T13:00:00+01:00'),
    )
    assert result == {'Mario': {('ONE', '1', '1'): 2, ('TWO', '1', '1'): 2}}


def test_open_multiple_group_splits_hours_between_members():
    rows, members = [], []
    for document in ('ONE', 'TWO'):
        row = event('2026-11-24T08:00:00+01:00', 'Attivo', document)
        row.IdDocumento = document
        rows.append(row)
        members.append(Row(IdDocumento=document, IdRiga='1', Fase='1', Role='MEMBER',
                           TimeShareMode='SPLIT', GroupUid='MULT-1', CreatedAt=row.ClosedAt,
                           ClosedAt='', DissolvedAt=''))
    result = service._calculate_worked_hours_by_user_phase(
        input_logs=rows, runtime_map={}, masked_members=members,
        start_dt=dt('2026-11-24T00:00:00+01:00'), end_dt=dt('2026-11-24T12:00:00+01:00'),
    )
    assert result == {'Mario': {('ONE', '1', '1'): 2, ('TWO', '1', '1'): 2}}


def test_produced_quantities_count_steps_once_and_only_finish_on_last_phase():
    rows = [event('2026-11-24T08:00:00+01:00', 'Attivo', 'a'),
            event('2026-11-24T10:00:00+01:00', 'Pianificata', 's', phase='2',
                  FaseConsuntivata='1', GestioneMatricola='no', QuantitaConforme='10', QuantitaNonConforme='1'),
            event('2026-11-24T11:00:00+01:00', 'Attivo', 'b', phase='2'),
            event('2026-11-24T12:00:00+01:00', 'In Sospeso', 'p', phase='2',
                  FaseConsuntivata='2', GestioneMatricola='no', QuantitaConforme='2', QuantitaNonConforme='1'),
            event('2026-11-24T13:00:00+01:00', 'Chiusa', 'f', phase='2',
                  FaseConsuntivata='2', GestioneMatricola='no', QuantitaConforme='3', QuantitaNonConforme='0')]
    produced, finished = service._produced_quantities(
        rows, {}, dt('2026-11-24T00:00:00+01:00'), dt('2026-11-24T14:00:00+01:00'),
    )
    assert produced['Mario'] == {('DOC', '1', '1'): 11, ('DOC', '1', '2'): 6}
    assert finished['Mario'] == {('DOC', '1', '2'): 6}
    rows[-1].ClosedAt = '2026-11-30T13:00:00+01:00'
    _, finished = service._produced_quantities(
        rows, {}, dt('2026-11-24T00:00:00+01:00'), dt('2026-11-24T14:00:00+01:00'),
    )
    assert finished['Mario'][('DOC', '1', '2')] == 3


def test_machine_partial_component_closures_do_not_count_finished_machines():
    partial = event('2026-11-24T10:00:00+01:00', 'In Sospeso', 'p', phase='2',
                    FaseConsuntivata='2', GestioneMatricola='si', ChiusuraParziale='true',
                    QuantitaConforme='1', QuantitaNonConforme='0')
    final = event('2026-11-24T12:00:00+01:00', 'Chiusa', 'f', phase='2',
                  FaseConsuntivata='2', GestioneMatricola='si', ChiusuraParziale='false',
                  QuantitaConforme='1', QuantitaNonConforme='0')
    produced, finished = service._produced_quantities(
        [partial, final], {}, dt('2026-11-24T00:00:00+01:00'), dt('2026-11-24T14:00:00+01:00'),
    )
    assert produced['Mario'] == finished['Mario'] == {('DOC', '1', '2'): 1}


def test_report_counts_quantity_even_when_closure_has_no_worked_hours(monkeypatch):
    row = event('2026-11-24T12:00:00+01:00', 'Chiusa', 'f', phase='2', FaseConsuntivata='2',
                GestioneMatricola='no', QuantitaConforme='3', QuantitaNonConforme='1',
                CodArt='ART', DesArt='Articolo', RifRegistraz='REG', StatoOrdinePre='In Sospeso')
    monkeypatch.setattr(service, '_load_user_by_id', lambda user_id: Row(id=1, username='Mario'))
    monkeypatch.setattr(service, '_load_deleted_order_keys', lambda: set())
    monkeypatch.setattr(service, '_load_runtime_logs_in_period', lambda **kwargs: [])
    monkeypatch.setattr(service, '_load_input_logs_in_period_for_user', lambda **kwargs: [row])
    monkeypatch.setattr(service, '_load_active_runtime_order_keys_for_user', lambda **kwargs: set())
    monkeypatch.setattr(service, '_load_input_logs_for_orders', lambda keys: [row])
    monkeypatch.setattr(service, '_weekly_capacity_hours', lambda user_id: 40)
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    monkeypatch.setattr(service, 'db', Row(session=session))
    report = service.build_report_settimanale_for_user(
        selected_user_id=1, start_dt=dt('2026-11-24T00:00:00+01:00'),
        end_dt=dt('2026-11-24T14:00:00+01:00'), can_select_user=False, users=[],
    )
    assert report['kpi']['componenti_semilavorati'] == 4
    assert report['kpi']['ordini_lavorati'] == 1
    assert report['kpi']['ore_impiegate'] == 0
    assert report['ordini_lavorati'][0]['qta_finale'] == 4


def test_full_report_against_isolated_sqlite_databases():
    from flask import Flask
    from sqlalchemy.schema import CreateTable
    from app_odp.models import db, User, ProductionCapacityCalendar, InputOdpLog, OdpRuntimeLog

    app = Flask(__name__)
    app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', SQLALCHEMY_BINDS={'log': 'sqlite://'},
                      SQLALCHEMY_TRACK_MODIFICATIONS=False)
    db.init_app(app)
    with app.app_context():
        # Lo schema esistente dichiara alcuni indici duplicati: qui servono solo tabelle isolate.
        for bind_key, engine in db.engines.items():
            with engine.begin() as connection:
                for table in db.metadatas[bind_key].sorted_tables:
                    connection.execute(CreateTable(table))
        user = User(username='ReportTester', public_id='report-tester', active=True)
        db.session.add(user)
        db.session.flush()
        for weekday in range(5):
            db.session.add(ProductionCapacityCalendar(scope_type='operatore', scope_code=str(user.id),
                                                       weekday=weekday, hours_capacity=8, active=True))

        def add(document, at, state, operation, phase, produced=None, seconds=0, masked=False):
            values = dict(OperationGroupId=operation, IdDocumento=document, IdRiga='1',
                          RifRegistraz=document, NumFase='[1,2]', FaseAttiva=phase,
                          ClosedBy=user.username, ClosedAt=f'2026-11-24T{at}:00:00+01:00',
                          StatoOrdinePre='Attivo', StatoOrdinePost=state, GestioneMatricola='no',
                          CodArt='ART', DesArt='Articolo', Quantita='100')
            payload = {'member_role': 'MASKED' if masked else 'MAIN', 'is_last_phase': phase == '2'}
            if produced is not None:
                values.update(FaseConsuntivata=phase, QuantitaConforme=str(produced), QuantitaNonConforme='0',
                              TempoFunzionamentoFinale='0.5' if masked else str(seconds / 3600))
            db.session.add(InputOdpLog(**values))
            db.session.add(OdpRuntimeLog(OperationGroupId=operation, IdDocumento=document, IdRiga='1',
                                        EventAt=values['ClosedAt'], Azione='chiusura' if produced is not None else 'avvio',
                                        PayloadJson=json.dumps(payload), ElapsedSeconds=str(seconds)))
        add('NORMAL', '08', 'Attivo', 'n-a', '1')
        add('NORMAL', '10', 'Pianificata', 'n-1', '1', 4, 7200)
        add('NORMAL', '10', 'Attivo', 'n-b', '2')
        add('NORMAL', '11', 'In Sospeso', 'n-p', '2', 2, 3600)
        add('NORMAL', '11', 'Attivo', 'n-c', '2')
        add('NORMAL', '12', 'Chiusa', 'n-f', '2', 2, 3600)
        add('MASK', '08', 'Attivo', 'm-a', '2', masked=True)
        add('MASK', '12', 'Chiusa', 'm-f', '2', 3, masked=True)
        db.session.commit()
        report = service.build_report_settimanale_for_user(
            selected_user_id=user.id, start_dt=dt('2026-11-23T00:00:00+01:00'),
            end_dt=dt('2026-11-25T00:00:00+01:00'), can_select_user=False, users=[],
        )
        assert report['ok']
        assert report['kpi']['ore_impiegate'] == 4
        assert report['kpi']['coefficiente_impiego'] == 10
        assert report['kpi']['ore_capacita_produttiva'] == 40
        assert report['kpi']['componenti_semilavorati'] == 7
        assert report['kpi']['ordini_lavorati'] == 2
        assert len(report['ordini_lavorati']) == 3
        assert next(row for row in report['ordini_lavorati'] if row['id_documento'] == 'MASK')['ore_mascherate'] == 0.5
        db.session.remove()
