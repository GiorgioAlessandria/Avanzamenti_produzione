from io import BytesIO

import pytest
from openpyxl import load_workbook

from test_serbatoi_cliente_flow import app
from app_odp.models import db
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as s


def shipped(cliente_id, quantity=10, date='2026-11-25T10:00:00', ddt='DDT-SPEDIZIONE', **values):
    lotto = s.Lotto(riferimento=f'SPED-{cliente_id}-{ddt}', tipo='Materiale', codice='OSS',
                    articolo_id=1, produzione_id=1, cliente_id=cliente_id, quantita=quantity,
                    iniziale=quantity, stato='Spedito', ubicazione='Cliente', **values)
    db.session.add(lotto)
    db.session.flush()
    db.session.add(s.Movimento(tipo='SPEDISCI', data=date, operatore='Tester', quantita=quantity,
                               destinazione_id=lotto.id, produzione_id=1, ddt=ddt))
    db.session.commit()
    return lotto


def context(app, monkeypatch, query):
    captured = {}
    def render(template, **values):
        captured.update(values)
        return 'OK'
    monkeypatch.setattr(routes, 'render_template', render)
    response = app.test_client().get('/serbatoi?' + query)
    assert response.status_code == 200
    return captured


def workbook(app, query):
    response = app.test_client().get('/serbatoi?' + query + '&export=xlsx')
    assert response.status_code == 200
    return load_workbook(BytesIO(response.data))


def test_completed_orders_leave_proposals_but_partial_orders_stay_and_history_remains_accessible(app, monkeypatch):
    shipped(2)
    shipped(1, 4)
    result = context(app, monkeypatch, 'tab=clienti')
    assert [r['id'] for r in result['rows']] == [1]
    assert [c.id for c in result['clienti']] == [1]
    assert {r['id'] for r in result['ordini_per_fase']['spedito']} == {1, 2}
    result = context(app, monkeypatch, 'tab=clienti&id=2')
    assert result['selected']['id'] == 2
    assert 2 not in [r['id'] for r in result['rows']]


def test_shipped_flow_shows_ten_most_recent_rows_by_shipment_date_not_id(app, monkeypatch):
    expected = []
    for number in range(3, 15):
        db.session.add(s.Cliente(id=number, id_documento=f'C{number}', id_riga='1', riferimento=f'CLIENTE-{number}',
                                cliente='Cliente', articolo_id=1, quantita=10, saldo_erp=10))
        db.session.flush()
        day = 29 - number
        lotto = shipped(number, date=f'2026-11-{day:02d}T10:00:00', ddt=f'DDT-{number}')
        expected.append(lotto.id)
    result = context(app, monkeypatch, 'tab=storico&fase=spedito')
    assert [r['id'] for r in result['ordini_per_fase']['spedito']] == expected[:10]
    assert [r['id'] for r in result['rows']] == expected[:10]
    assert result['totali_fasi']['spedito'] == 100
    result = context(app, monkeypatch, 'tab=spediti')
    assert [r['id'] for r in result['rows']] == expected


def provenance():
    order = s.Barre(fornitore_tipo='F', fornitore_codice='123', fornitore_nome='Fornitore', riferimento='ORD-BARRE-77',
                    codice='BARRA', lunghezza_mm=6000, quantita=1)
    db.session.add(order)
    db.session.flush()
    barra = s.Lotto(riferimento='BARRA-ESAURITA', tipo='Barra', codice='BARRA', quantita=0, iniziale=1,
                    stato='Disponibile', ubicazione='Bernardi', barre_id=order.id)
    db.session.add(barra)
    db.session.flush()
    spezzone = s.Lotto(riferimento='SPEZZONE-ESAURITO', tipo='Spezzone', codice='SPEZ', quantita=0, iniziale=10,
                       stato='Disponibile', ubicazione='Bernardi', padre_id=barra.id)
    db.session.add(spezzone)
    db.session.flush()
    db.session.add(s.Consumo(produzione_id=1, lotto_id=spezzone.id, quantita=10))
    for tipo, origine, destinazione, ddt in [('RICEZIONE_BARRE', None, barra.id, 'DDT-BARRE-77'),
                                            ('TAGLIO', barra.id, spezzone.id, 'DDT-SPEZZONI-88')]:
        db.session.add(s.Movimento(tipo=tipo, origine_id=origine, destinazione_id=destinazione,
                                   data='2026-11-01T08:00:00', operatore='Tester', quantita=1, ddt=ddt))
    db.session.commit()


@pytest.mark.parametrize('query', ['ORD-BARRE-77', 'DDT-BARRE-77', 'DDT-SPEZZONI-88', 'ODP-1', 'DDT-SPEDIZIONE'])
def test_archive_searches_all_order_and_ddt_references_including_exhausted_sources(app, monkeypatch, query):
    provenance()
    lotto = shipped(2)
    result = context(app, monkeypatch, 'tab=spediti&q=' + query)
    assert [r['id'] for r in result['rows']] == [lotto.id]
    assert result['rows'][0]['riferimenti']['ddt'] == ['DDT-BARRE-77', 'DDT-SPEDIZIONE', 'DDT-SPEZZONI-88']


def test_archive_renders_readonly_details_and_references(app):
    provenance()
    lotto = shipped(2)
    response = app.test_client().get(f'/serbatoi?tab=spediti&id={lotto.id}')
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    for value in ('ORD-BARRE-77', 'DDT-BARRE-77', 'DDT-SPEZZONI-88', 'DDT-SPEDIZIONE', 'ODP-1', 'Movimenti e documenti'):
        assert value in html
    assert 'name="azione" value="avanza"' not in html
    assert 'Aggiornamento automatico ogni 10 secondi.' in html


def test_export_respects_phase_and_search_with_numeric_quantities_codes_and_real_dates(app):
    cliente = db.session.get(s.Cliente, 1)
    cliente.consegna = '2026-12-04'
    db.session.commit()
    book = workbook(app, 'tab=clienti&fase=gaser&q=CLIENTE-1')
    values = list(book.active.values)
    assert values[0] == ('Ordine', 'Riga', 'Codice', 'Codice ossidato', 'Quantità', 'Data di consegna', 'Fase')
    assert len(values) == 2
    assert values[1][:5] == ('CLIENTE-1', '1', 'LAV', 'OSS', 10)
    assert values[1][5].date().isoformat() == '2026-12-04'
    assert values[1][6] == 'Gaser'
    assert len(list(workbook(app, 'tab=clienti&q=inesistente').active.values)) == 1


def test_archive_export_keeps_all_filtered_shipments_and_literal_order_references(app):
    cliente = db.session.get(s.Cliente, 2)
    cliente.riferimento = '=1+1'
    db.session.commit()
    shipped(2)
    shipped(1, 4, ddt='ALTRO-DDT')
    book = workbook(app, 'tab=spediti&q=DDT-SPEDIZIONE')
    assert len(list(book.active.values)) == 2
    assert book.active['A2'].value == '=1+1' and book.active['A2'].data_type == 's'
    assert book.active['E2'].value == 10
    assert book.active['G2'].value == 'Spedito'


def test_production_export_preserves_unclassified_orders_and_customer_delivery(app):
    cliente = db.session.get(s.Cliente, 1)
    cliente.consegna = '2026-12-04'
    db.session.commit()
    values = list(workbook(app, 'tab=produzione').active.values)
    assert len(values) > 1
    assert values[1][0:4] == ('ODP-1', '1', 'LAV', 'OSS')
    assert values[1][5].date().isoformat() == '2026-12-04'


def test_client_search_accepts_worked_code_and_export_contains_all_unassigned_pieces(app):
    values = list(workbook(app, 'tab=clienti&q=LAV&fase=da_assegnare').active.values)
    assert len(values) == 2
    assert values[1][0] == 'CLIENTE-2'
    assert values[1][4] == 10
    assert values[1][6] == 'Da assegnare'
