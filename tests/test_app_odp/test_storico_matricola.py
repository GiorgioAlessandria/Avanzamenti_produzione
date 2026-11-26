import json

import pytest
from test_storico_ordini_helpers import storico_app, history_app, runtime_event
from app_odp.models import db, InputOdpLog
from app_odp.services import storico_ordini_service as service


def document(serial, row='1', document='DOC'):
    return InputOdpLog(IdDocumento=document, IdRiga=row, RifRegistraz=document,
                       OperationGroupId=f'doc-{document}-{row}', CodMatricola=serial,
                       ClosedAt='2026-10-30T12:00:00', CodArt='ART', FaseConsuntivata='20')


def test_serial_from_document_finds_all_passages_including_events_without_serial(history_app):
    with history_app.app_context():
        db.session.add_all([document('000123'), runtime_event('presa_in_carico', '08'),
                            runtime_event('sospensione', '09'), runtime_event('riattivazione', '10'),
                            runtime_event('chiusura_finale', '11', fase_successiva='30'),
                            document('0001234', row='2')])
        db.session.commit()
        result = service.build_storico_ordini_list({'matricola': ' 000123 '})
        assert result['total'] == 1
        assert result['rows'][0]['key'] == 'order:DOC|1'
        assert result['rows'][0]['count'] == 5
        assert result['rows'][0]['matricole'] == ['000123']
        detail = service.build_storico_ordini_detail({'id_documento': 'DOC', 'id_riga': '1'})
        assert [r['azione'] for r in detail['timeline'][:4]] == [
            'presa_in_carico', 'sospensione', 'riattivazione', 'chiusura_finale']


@pytest.mark.parametrize('key', ['matricola', 'cod_matricola'])
def test_serial_in_runtime_payload_finds_input_and_runtime_history(history_app, key):
    with history_app.app_context():
        early = runtime_event('presa_in_carico', '08')
        close = runtime_event('chiusura_macchina', '11', **{key: ' AB-001 '})
        db.session.add_all([early, close, document(''), document('AB-001', row='2', document='ALTRO')])
        db.session.commit()
        result = service.build_storico_ordini_list({'matricola': 'ab-001', 'q': 'DOC'})
        assert result['total'] == 1
        assert result['rows'][0]['count'] == 3
        assert result['rows'][0]['matricole'] == ['AB-001']


def test_exact_serial_matching_treats_percent_underscore_and_quotes_as_literal(history_app):
    with history_app.app_context():
        for row, serial in [('1', 'A_%"01'), ('2', 'AXYZ01'), ('3', 'A_%"010')]:
            db.session.add(document(serial, row=row))
        db.session.commit()
        result = service.build_storico_ordini_list({'matricola': 'A_%"01'})
        assert result['total'] == 1
        assert result['rows'][0]['key'] == 'order:DOC|1'


def test_invalid_or_null_runtime_payload_does_not_break_search(history_app):
    with history_app.app_context():
        for row, payload in [('1', '{bad'), ('2', None), ('3', 'null')]:
            event = runtime_event('presa_in_carico', '08', row=row)
            event.PayloadJson = payload
            db.session.add(event)
        db.session.add(document('MATCH'))
        db.session.commit()
        assert service.build_storico_ordini_list({'matricola': 'MATCH'})['total'] == 1
        assert service.build_storico_ordini_list({'matricola': 'NOT-FOUND'})['total'] == 0


def test_serial_matches_whole_order_key_without_leaking_other_rows_or_documents(history_app):
    with history_app.app_context():
        db.session.add_all([document('MATCH'), document('OTHER', row='2'), document('OTHER', document='DOC2'),
                            runtime_event('presa_in_carico', '08'), runtime_event('presa_in_carico', '08', row='2')])
        db.session.commit()
        result = service.build_storico_ordini_list({'matricola': 'MATCH'})
        assert result['total'] == 1
        assert result['rows'][0]['key'] == 'order:DOC|1'


def test_explicit_period_and_other_filters_are_respected_even_if_serial_registered_later(history_app):
    with history_app.app_context():
        early = runtime_event('presa_in_carico', '08')
        early.EventAt = '2026-01-05T08:00:00'
        db.session.add_all([early, document('OLD-SERIAL')])
        db.session.commit()
        assert service.build_storico_ordini_list({'matricola': 'OLD-SERIAL'})['rows'][0]['count'] == 2
        january = service.build_storico_ordini_list({'matricola': 'OLD-SERIAL', 'date_from': '2026-01-01', 'date_to': '2026-01-31'})
        assert january['total'] == 1 and january['rows'][0]['count'] == 1
        assert service.build_storico_ordini_list({'matricola': 'OLD-SERIAL', 'articolo': 'OTHER'})['total'] == 0
        assert service.build_storico_ordini_list({'matricola': 'OLD-SERIAL', 'date_from': '2027-01-01'})['total'] == 0


def test_matching_happens_before_scan_limit(history_app, monkeypatch):
    with history_app.app_context():
        monkeypatch.setattr(service, 'SCAN_LIMIT', 2)
        db.session.add(document('TARGET'))
        for number in range(2, 8):
            other = document('OTHER', row=str(number))
            other.ClosedAt = '2026-11-25T12:00:00'
            db.session.add(other)
        db.session.commit()
        result = service.build_storico_ordini_list({'matricola': 'TARGET'})
        assert result['total'] == 1
        assert result['rows'][0]['key'] == 'order:DOC|1'
        assert not result['scan_limit_reached']
