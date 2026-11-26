import json
from types import SimpleNamespace

import pytest
from openpyxl import load_workbook

from test_storico_ordini_helpers import storico_app, history_app, runtime_event
from test_storico_matricola import document
from app_odp.models import db, LottiUsatiLog, LottiGeneratiLog
from app_odp.routes_modules import storico_ordini as routes
from app_odp.services import storico_ordini_service as storico
from app_odp.services.storico_matricole_export import build_matricole_excel, parse_matricole


def records(sheet, kind=None):
    rows = list(sheet.values)
    values = [dict(zip(rows[0], row)) for row in rows[1:]]
    return [row for row in values if kind is None or row['Tipo record'] == kind]


def test_list_preserves_leading_zeros_removes_duplicates_and_supports_excel_columns():
    assert parse_matricole(' 00123\r\nAB-1; ab-1, Z9\t00123\nA 001 ') == ['00123', 'AB-1', 'Z9', 'A 001']
    with pytest.raises(ValueError, match='Inserire'):
        parse_matricole(' \n;,\t')


def test_workbook_includes_every_passage_documents_lots_and_missing_serials(history_app):
    with history_app.app_context():
        take = runtime_event('presa_in_carico', '08')
        take.EventAt = '2020-01-01T08:00:00'
        close = runtime_event('chiusura_macchina', '11', matricola='000123')
        close.QuantitaConforme = '1'
        closed = document('000123')
        closed.OperationGroupId, closed.ClosedAt = close.OperationGroupId, close.EventAt
        closed.TempoFunzionamentoFinale, closed.QuantitaConforme = '1.5', '1'
        db.session.add_all([take, close, closed,
            LottiUsatiLog(IdDocumento='DOC', IdRiga='1', OperationGroupId=close.OperationGroupId,
                         CodArt='COMP', RifLottoAlfa='LOT-01', Quantita='2.5', Fase='20', ClosedAt=close.EventAt),
            LottiGeneratiLog(IdDocumento='DOC', IdRiga='1', OperationGroupId=close.OperationGroupId,
                            CodArt='ART', RifLottoAlfa='000123', Quantita='1', Fase='20', ClosedAt=close.EventAt)])
        db.session.commit()
        book = load_workbook(build_matricole_excel('000123\n000123\nASSENTE'))
        assert book.sheetnames == ['Storico matricole']
        sheet = book.active
        headers = next(sheet.values)
        assert len(headers) == len(set(headers))
        summary = records(sheet, 'Riepilogo ordine') + records(sheet, 'Matricola non trovata')
        assert len(summary) == 2
        assert summary[0]['Matricola richiesta'] == '000123' and summary[0]['Numero eventi'] == 2
        assert summary[1]['Esito'] == 'Non presente nello storico'
        passages = records(sheet, 'Passaggio')
        assert len(passages) == 2
        assert passages[0]['Data e ora (Roma)'].year == 2020
        assert passages[-1]['Quantità OK'] == 1
        assert passages[-1]['Lavorazione (ore)'] == 1.5
        assert json.loads(passages[-1]['Dati completi evento (JSON)'])['payload']['matricola'] == '000123'
        assert records(sheet, 'Avanzamento')[0]['Matricola documento'] == '000123'
        assert records(sheet, 'Lotto usato')[0]['Quantità'] == 2.5
        assert records(sheet, 'Lotto generato')[0]['Lotto / matricola'] == '000123'
        assert sheet.freeze_panes == 'G2'
        assert sheet.auto_filter.ref.endswith('8')


def test_export_returns_all_orders_without_list_scan_or_pagination_limits(history_app, monkeypatch):
    with history_app.app_context():
        monkeypatch.setattr(storico, 'SCAN_LIMIT', 1)
        for row in ('1', '2', '3'):
            db.session.add(document('SAME', row=row))
        db.session.commit()
        book = load_workbook(build_matricole_excel('SAME'))
        assert len(records(book.active, 'Riepilogo ordine')) == 3
        assert len(records(book.active, 'Avanzamento')) == 3


def test_multiple_serials_sharing_order_are_each_reported_and_export_keeps_literal_text(history_app):
    with history_app.app_context():
        db.session.add_all([document('=1+1'), runtime_event('chiusura_macchina', '11', matricola='OTHER')])
        db.session.commit()
        book = load_workbook(build_matricole_excel('=1+1;OTHER'))
        summary = records(book.active, 'Riepilogo ordine')
        assert [row['Matricola richiesta'] for row in summary] == ['=1+1', 'OTHER']
        assert book.active['B2'].data_type == 's'
        assert len(records(book.active, 'Passaggio')) == 4


def test_long_payloads_are_preserved_in_chunks_and_invalid_xml_characters_are_escaped(history_app):
    with history_app.app_context():
        event = runtime_event('chiusura_macchina', '11', matricola='LONG', note_lunghe='x' * 40000)
        event.Note = 'Nota\x01di prova'
        db.session.add(event)
        db.session.commit()
        book = load_workbook(build_matricole_excel('LONG'))
        assert book.sheetnames == ['Storico matricole']
        passage = records(book.active, 'Passaggio')[0]
        assert passage['Note'] == 'Nota\\x01di prova'
        assert 'Testo esteso' in passage['Dati completi evento (JSON)']
        chunks = records(book.active, 'Testo esteso')
        payload = json.loads(''.join(row['Testo esteso'] for row in chunks))
        assert payload['payload']['note_lunghe'] == 'x' * 40000


@pytest.fixture
def export_client(history_app, monkeypatch):
    from app_odp.policy import decorator
    monkeypatch.setattr(decorator, 'resolve_operator_session', lambda: SimpleNamespace())
    monkeypatch.setattr(decorator, 'active_policy', lambda: SimpleNamespace(can=lambda code: code == 'storico_ordini'))
    history_app.register_blueprint(routes.main_bp)
    return history_app.test_client()


def test_export_endpoint_accepts_post_returns_xlsx_and_rejects_empty_input(export_client):
    response = export_client.post('/api/storico-ordini/matricole/export?tab_session=test', data={'matricole': 'ASSENTE'})
    assert response.status_code == 200
    assert 'spreadsheetml.sheet' in response.content_type
    assert 'storico_matricole.xlsx' in response.headers['Content-Disposition']
    assert export_client.post('/api/storico-ordini/matricole/export', data={}).status_code == 400


def test_export_endpoint_respects_history_permission(export_client, monkeypatch):
    from app_odp.policy import decorator
    monkeypatch.setattr(decorator, 'active_policy', lambda: SimpleNamespace(can=lambda code: False))
    assert export_client.post('/api/storico-ordini/matricole/export', data={'matricole': '123'}).status_code == 403


def test_page_renders_batch_section_with_operator_session(export_client, history_app, monkeypatch):
    from pathlib import Path
    from flask import url_for
    from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
    history_app.jinja_loader = ChoiceLoader([
        DictLoader({'base.j2': '{% block styles %}{% endblock %}{% block content %}{% endblock %}{% block extra_js %}{% endblock %}'}),
        FileSystemLoader(str(Path(routes.__file__).resolve().parents[1] / 'templates')),
    ])
    history_app.jinja_env.globals['operator_url_for'] = lambda endpoint, **kwargs: url_for(endpoint, tab_session='test', **kwargs)
    monkeypatch.setattr(routes, 'active_token', lambda: 'test')
    response = export_client.get('/storico-ordini?tab_session=test')
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'name="matricole"' in html
    assert '/api/storico-ordini/matricole/export?tab_session=test' in html
    assert 'Un unico foglio contiene tutte le matricole' in html
