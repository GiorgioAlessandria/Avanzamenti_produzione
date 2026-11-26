from datetime import datetime
from types import SimpleNamespace as Row
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from app_odp.services import report_settimanale_service as service


@pytest.mark.parametrize('today, first, last', [
    ('2026-11-25T15:24:12', '2026-11-18', '2026-11-25'),
    ('2026-12-01T00:00:00', '2026-11-24', '2026-12-01'),
    ('2027-01-01T23:59:59', '2026-12-25', '2027-01-01'),
    ('2026-10-26T12:00:00', '2026-10-19', '2026-10-26'),
])
def test_range_is_seven_complete_local_dates(monkeypatch, today, first, last):
    from app_odp.routes_modules import report_settimanale as routes
    now = datetime.fromisoformat(today).replace(tzinfo=ZoneInfo('Europe/Rome'))
    monkeypatch.setattr(routes, '_now_rome', lambda: now)
    start, end = routes._last_7_days_range()
    assert start.date().isoformat() == first
    assert end.date().isoformat() == last
    assert start.hour == end.hour == start.minute == end.minute == 0
    assert (end.date() - start.date()).days == 7


def test_quantity_boundaries_include_first_midnight_and_exclude_today():
    def closure(at, operation):
        return Row(IdDocumento='DOC', IdRiga='1', FaseConsuntivata='1', NumFase='1',
                   ClosedAt=at, ClosedBy='Mario', OperationGroupId=operation,
                   StatoOrdinePost='Chiusa', GestioneMatricola='no',
                   QuantitaConforme='3', QuantitaNonConforme='0')
    produced, finished = service._produced_quantities([
        closure('2026-11-18T00:00:00+01:00', 'first'),
        closure('2026-11-24T23:59:59+01:00', 'last'),
        closure('2026-11-25T00:00:00+01:00', 'today'),
    ], {}, datetime.fromisoformat('2026-11-18T00:00:00+01:00'),
       datetime.fromisoformat('2026-11-25T00:00:00+01:00'))
    assert produced['Mario'] == finished['Mario'] == {('DOC', '1', '1'): 6}


def test_old_order_closed_today_keeps_only_the_previous_seven_days(monkeypatch):
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromisoformat('2026-11-25T12:00:00+01:00').astimezone(tz)
    monkeypatch.setattr(service, 'datetime', FrozenDatetime)
    start = datetime.fromisoformat('2026-11-18T00:00:00+01:00')
    end = datetime.fromisoformat('2026-11-25T00:00:00+01:00')
    active = Row(IdDocumento='DOC', IdRiga='1', NumFase='1', FaseAttiva='1',
                 OperationGroupId='a', ClosedBy='Mario', ClosedAt='2026-11-17T23:00:00+01:00',
                 StatoOrdinePre='Pianificata', StatoOrdinePost='Attivo',
                 CodArt='ART', DesArt='Articolo', GestioneMatricola='no', RifRegistraz='REG')
    closed = Row(**{**vars(active), 'OperationGroupId': 'c', 'ClosedAt': '2026-11-25T02:00:00+01:00',
                    'StatoOrdinePost': 'Chiusa', 'FaseConsuntivata': '1',
                    'QuantitaConforme': '5', 'QuantitaNonConforme': '0'})
    runtime = Row(IdDocumento='DOC', IdRiga='1', OperationGroupId='c', EventAt=closed.ClosedAt,
                  Azione='chiusura', ElapsedSeconds=str(171 * 3600), PayloadJson='{}',
                  StatoOrdinePre='Attivo', StatoOrdinePost='Chiusa')
    monkeypatch.setattr(service, '_load_user_by_id', lambda user_id: Row(id=1, username='Mario'))
    monkeypatch.setattr(service, '_load_deleted_order_keys', lambda: set())
    monkeypatch.setattr(service, '_load_runtime_logs_in_period',
                        lambda **kw: [runtime] if runtime.EventAt <= kw['end_iso'] else [])
    monkeypatch.setattr(service, '_load_input_logs_in_period_for_user',
                        lambda **kw: [closed] if closed.ClosedAt <= kw['end_iso'] else [])
    monkeypatch.setattr(service, '_load_active_runtime_order_keys_for_user', lambda **kw: set())
    monkeypatch.setattr(service, '_load_input_logs_for_orders', lambda keys: [active, closed] if keys else [])
    monkeypatch.setattr(service, '_weekly_capacity_hours', lambda user_id: 40)
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    monkeypatch.setattr(service, 'db', Row(session=session))
    report = service.build_report_settimanale_for_user(
        selected_user_id=1, start_dt=start, end_dt=end, can_select_user=False, users=[],
    )
    assert report['kpi']['ore_impiegate'] == 168
    assert report['kpi']['componenti_semilavorati'] == 0
    assert report['ordini_lavorati'][0]['stato'] == 'Attivo'
    assert report['periodo'] == {'start': '18/11/2026', 'end': '24/11/2026'}
