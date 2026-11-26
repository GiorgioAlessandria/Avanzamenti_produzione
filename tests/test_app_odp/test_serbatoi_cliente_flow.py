from types import SimpleNamespace

import pytest
from flask import Flask
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
from sqlalchemy.schema import CreateTable

from app_odp.models import db
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as service


@pytest.fixture
def app(monkeypatch):
    app = Flask(__name__)
    app.config.update(SECRET_KEY='test', TESTING=True, SQLALCHEMY_DATABASE_URI='sqlite://',
                      SQLALCHEMY_BINDS={key: 'sqlite://' for key in db.metadatas if key is not None}, SQLALCHEMY_TRACK_MODIFICATIONS=False)
    db.init_app(app)
    policy = SimpleNamespace(can=lambda code: code == 'produzione_serbatoi', has_direct_admin_role=False)
    monkeypatch.setattr(routes, 'active_policy', lambda: policy)
    monkeypatch.setattr(routes, 'active_user', lambda: SimpleNamespace(id=1, active=True))
    monkeypatch.setattr(routes, 'active_token', lambda: None)
    monkeypatch.setattr(routes, '_current_username', lambda: 'Tester')
    from app_odp.policy import decorator
    monkeypatch.setattr(decorator, 'resolve_operator_session', lambda: SimpleNamespace())
    monkeypatch.setattr(decorator, 'active_policy', lambda: policy)
    app.register_blueprint(routes.main_bp)
    from pathlib import Path
    app.jinja_loader = ChoiceLoader([
        DictLoader({'base.j2': '{% block styles %}{% endblock %}{% block content %}{% endblock %}{% block extra_js %}{% endblock %}'}),
        FileSystemLoader(str(Path(routes.__file__).resolve().parents[1] / 'templates')),
    ])
    from flask import url_for
    app.jinja_env.globals['operator_url_for'] = url_for
    with app.app_context():
        for key, engine in db.engines.items():
            with engine.begin() as connection:
                for table in db.metadatas[key].sorted_tables:
                    connection.execute(CreateTable(table))
        article = service.Articolo(codice_oss='OSS', codice_lav='LAV', codice_spezzone='SPEZ', descrizione='Serbatoio')
        db.session.add(article)
        db.session.flush()
        for number in (1, 2):
            db.session.add(service.Cliente(id=number, id_documento=f'C{number}', id_riga='1',
                                           riferimento=f'CLIENTE-{number}', cliente=f'Azienda {number}',
                                           articolo_id=article.id, quantita=10, saldo_erp=10))
        db.session.add(service.Produzione(id=1, id_documento='P1', id_riga='1', riferimento='ODP-1',
                                         riferimento_stock='STOCK-1', articolo_id=article.id, quantita=10,
                                         fase_inizio='1', fase_fine='2'))
        db.session.add(service.Impegno(id=1, cliente_id=1, produzione_id=1, quantita=10, realizzata=10))
        for number, state, place in ((1, 'Lavorato', 'Bernardi'), (2, 'Lavorato', 'Gaser'), (3, 'Ossidato', 'Bernardi')):
            db.session.add(service.Lotto(id=number, riferimento=f'LOTTO-{number}', tipo='Materiale',
                                         codice='LAV', articolo_id=article.id, produzione_id=1,
                                         cliente_id=1, quantita=10, iniziale=10, stato=state, ubicazione=place))
        db.session.commit()
        yield app
        db.session.remove()


def post(app, action='avanza', **values):
    client = app.test_client()
    with client.session_transaction() as session:
        session['serbatoi_csrf'] = 'csrf-test'
    data = dict(azione=action, csrf='csrf-test', richiesta='request-test', tab='clienti', id='1',
                lotto_id='1', passaggio='gaser', quantita='4', scarti='0', ddt='123')
    data.update(values)
    return client, client.post('/serbatoi/azione', data=data)


def test_customer_overview_contains_only_own_lots_and_available_transitions(app, monkeypatch):
    monkeypatch.setattr(service, 'origine_completa', lambda lotto: True)
    data = routes._operazioni_cliente(db.session.get(service.Cliente, 1), {1})
    assert [row['passaggio'][0] for row in data['lotti']] == ['spedisci', 'rientro', 'gaser']
    assert {row['lotto'].cliente_id for row in data['lotti']} == {1}
    assert len(data['produzioni']) == 1
    assert routes._operazioni_cliente(db.session.get(service.Cliente, 2), set())['lotti'] == []


@pytest.mark.parametrize('action, values', [
    ('avanza', {'lotto_id': '1'}), ('libera_materiale', {'lotto_id': '1'}),
    ('libera_impegno', {'impegno_id': '1'}), ('consuma', {'produzione_id': '1'}),
    ('assegna_odp', {'cliente_id': '1'}),
])
def test_customer_context_rejects_objects_from_another_order(app, action, values):
    with pytest.raises(service.SerbatoiError):
        routes._contesto_cliente(dict(tab='clienti', id='2', **values), action)


def test_partial_ddt_stays_on_customer_and_retries_do_not_duplicate(app, monkeypatch):
    monkeypatch.setattr(service, 'origine_completa', lambda lotto: True)
    client, response = post(app)
    assert response.status_code == 302
    assert 'tab=clienti' in response.location and 'id=1' in response.location
    assert db.session.get(service.Lotto, 1).quantita == 6
    moved = service.Lotto.query.filter_by(padre_id=1, ubicazione='Gaser').one()
    assert moved.quantita == 4 and moved.cliente_id == 1
    assert service.Movimento.query.filter_by(tipo='GASER').count() == 1
    repeated = client.post('/serbatoi/azione', data=dict(azione='avanza', csrf='csrf-test', richiesta='request-test',
                           tab='clienti', id='1', lotto_id='1', passaggio='gaser', quantita='4', scarti='0', ddt='123'))
    assert repeated.status_code == 302
    assert service.Movimento.query.filter_by(tipo='GASER').count() == 1


def test_invalid_quantity_and_wrong_customer_do_not_write_movements(app, monkeypatch):
    monkeypatch.setattr(service, 'origine_completa', lambda lotto: True)
    _, response = post(app, quantita='11')
    assert response.status_code == 302
    assert service.Movimento.query.count() == 0
    _, response = post(app, id='2')
    assert service.Movimento.query.count() == 0


def test_shipping_is_linked_to_customer_order(app, monkeypatch):
    monkeypatch.setattr(service, 'origine_completa', lambda lotto: True)
    _, response = post(app, lotto_id='3', passaggio='spedisci')
    assert 'tab=clienti' in response.location
    shipment = service.Lotto.query.filter_by(padre_id=3, stato='Spedito').one()
    assert shipment.cliente_id == 1 and shipment.quantita == 4
    assert service.Movimento.query.filter_by(destinazione_id=shipment.id).one().ddt == '123'


def test_return_stays_on_customer_and_exposes_quality_control(app, monkeypatch):
    monkeypatch.setattr(service, 'origine_completa', lambda lotto: True)
    _, response = post(app, lotto_id='2', passaggio='rientro')
    assert 'tab=clienti' in response.location
    returned = service.Lotto.query.filter_by(padre_id=2, stato=routes.cq.STATO_CQ).one()
    assert returned.cliente_id == 1 and returned.quantita == 4
    control = routes.cq.Controllo.query.filter_by(lotto_id=returned.id).one()
    assert control.cliente_id == 1
    row = next(row for row in routes._operazioni_cliente(db.session.get(service.Cliente, 1), {1})['lotti']
               if row['lotto'].id == returned.id)
    assert row['in_controllo'] and row['controllo'].id == control.id
    html = app.test_client().get(f'/serbatoi/qualita?id={control.id}').get_data(as_text=True)
    assert "Torna all&#39;ordine cliente" in html or "Torna all'ordine cliente" in html
    html = app.test_client().get('/serbatoi?tab=clienti&id=1').get_data(as_text=True)
    assert 'Apri controllo qualità' in html


def test_customer_page_contains_forms_and_other_pages_link_back(app, monkeypatch):
    monkeypatch.setattr(service, 'origine_completa', lambda lotto: True)
    client = app.test_client()
    response = client.get('/serbatoi?tab=clienti&id=1')
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert '1. Copertura ordine: ODP e magazzino' in html
    assert '3. DDT Gaser e spedizioni cliente' in html
    assert html.count('name="azione" value="avanza"') == 3
    assert 'name="azione" value="consuma"' not in html
    assert 'name="azione" value="libera_materiale"' not in html
    assert 'serbatoi-avanzamento-1' in html
    assert 'serbatoi-avanzamento-2' in html
    assert 'serbatoi-avanzamento-3' in html
    historical = client.get('/serbatoi?tab=storico&id=1').get_data(as_text=True)
    assert 'Gestisci DDT e materiale nella scheda cliente' in historical
    assert 'name="azione" value="avanza"' not in historical
    assert 'name="azione" value="libera_materiale"' not in historical
    production = client.get('/serbatoi?tab=produzione&id=1').get_data(as_text=True)
    assert 'name="azione" value="consuma"' not in production
    assert 'dalla sua scheda' in production
    assert client.get('/serbatoi?tab=barre').status_code == 403
    monkeypatch.setattr(routes, 'active_policy', lambda: SimpleNamespace(can=lambda code: True, has_direct_admin_role=False))
    warehouse = client.get('/serbatoi?tab=magazzino&id=1').get_data(as_text=True)
    assert 'Gestisci DDT e materiale nella scheda cliente' in warehouse
    assert 'name="azione" value="avanza"' not in warehouse
    assert 'name="azione" value="libera_materiale"' not in warehouse
    from html.parser import HTMLParser
    class FormParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_form = False
            self.ids = []
        def handle_starttag(self, tag, attrs):
            if tag == 'form':
                assert not self.in_form, 'Nested form'
                self.in_form = True
            if 'id' in dict(attrs):
                self.ids.append(dict(attrs)['id'])
        def handle_endtag(self, tag):
            if tag == 'form':
                assert self.in_form
                self.in_form = False
    parser = FormParser()
    parser.feed(html)
    assert not parser.in_form
    assert len(parser.ids) == len(set(parser.ids))


def test_stock_production_keeps_material_operations(app, monkeypatch):
    service.Impegno.query.delete()
    for lotto in service.Lotto.query.all():
        lotto.cliente_id = None
        lotto.stato = 'In lavorazione'
    db.session.commit()
    client = app.test_client()
    html = client.get('/serbatoi?tab=produzione&id=1').get_data(as_text=True)
    assert 'name="azione" value="consuma"' in html


def test_customer_documents_include_exhausted_lots_but_not_other_orders(app, monkeypatch):
    monkeypatch.setattr(service, 'origine_completa', lambda lotto: True)
    own = db.session.get(service.Lotto, 1)
    own.quantita = 0
    other = service.Lotto(riferimento='OTHER', tipo='Materiale', codice='LAV', articolo_id=own.articolo_id,
                          cliente_id=2, quantita=4, iniziale=4, stato='Lavorato', ubicazione='Gaser')
    db.session.add(other)
    db.session.flush()
    for lotto, ddt in ((own, '111'), (other, '222')):
        db.session.add(service.Movimento(tipo='GASER', data='2026-11-25', operatore='Tester',
                                        quantita=4, destinazione_id=lotto.id, ddt=ddt))
    db.session.flush()
    data = routes._operazioni_cliente(db.session.get(service.Cliente, 1), {1})
    assert [doc.ddt for doc in data['documenti']] == ['111']
    assert all(row['lotto'].id != own.id for row in data['lotti'])


def test_production_assigned_from_stock_is_operated_from_customer(app, monkeypatch):
    service.Impegno.query.delete()
    db.session.commit()
    html = app.test_client().get('/serbatoi?tab=produzione&id=1').get_data(as_text=True)
    assert 'name="azione" value="consuma"' not in html
    assert 'CLIENTE-1' in html
    assert 'Produzione per stock.' not in html
