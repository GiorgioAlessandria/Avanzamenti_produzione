from datetime import date
from io import BytesIO
from types import SimpleNamespace

from flask import render_template, request
from pypdf import PdfReader

from test_serbatoi_cliente_flow import app
from test_serbatoi_cqc_layout import complete_controls
from app_odp.models import db
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as service


def test_cqc_filename_and_removed_sentence(app):
    complete_controls()
    customer = db.session.get(service.Cliente, 1)
    customer.riferimento, customer.id_riga = '4517742580', '4'
    db.session.commit()
    response = app.test_client().get('/serbatoi/cqc/1')
    assert response.status_code == 200
    assert 'filename=4517742580_4.pdf' in response.headers['Content-Disposition']
    pdf = PdfReader(BytesIO(response.data))
    assert len(pdf.pages) == 1
    assert 'I risultati di fresatura' not in pdf.pages[0].extract_text()


def test_quality_stage_is_counted_separately_from_production():
    quantities = routes._fasi_da_stati({'In lavorazione · Bernardi': 4,
                                       f'{routes.cq.STATO_CQ} · Bernardi': 6,
                                       'Ossidato · Bernardi': 3})
    assert quantities['in_lavorazione'] == 4
    assert quantities['cq'] == 6
    assert quantities['ossidato'] == 3
    assert sum(quantities.values()) == 13
    assert [phase['nome'] for phase in routes.FASI][-3:] == ['In controllo', 'Controllato', 'Spedito']


def test_customer_shipping_and_warehouse_filters_show_pending_quality(app, monkeypatch):
    lot = db.session.get(service.Lotto, 2)
    lot.stato, lot.ubicazione = routes.cq.STATO_CQ, 'Bernardi'
    db.session.commit()
    monkeypatch.setattr(routes, 'active_policy', lambda: SimpleNamespace(can=lambda code: True, has_direct_admin_role=False))
    client = app.test_client()
    for tab in ('clienti', 'storico', 'magazzino'):
        response = client.get(f'/serbatoi?tab={tab}&fase=cq&id={1 if tab == "clienti" else 2}')
        assert response.status_code == 200
        html = response.get_data(as_text=True)
        assert 'In controllo' in html
        assert 'CLIENTE-1' in html
        assert 'Controllato' in html
        assert 'Ossidato · da spedire' not in html
    response = client.get('/serbatoi?tab=clienti&fase=ossidato')
    assert response.status_code == 200
    assert 'Controllato' in response.get_data(as_text=True)
    assert db.session.get(service.Lotto, 3).stato == 'Ossidato'


def test_separating_quality_stage_preserves_configured_delivery_estimates():
    cfg = SimpleNamespace(giorni={key: default for key, (_, default) in service.TEMPI_SERBATOI.items()},
                          lotti={'OSS': 10})
    before = dict(grezzo=0, da_assegnare=0, in_lavorazione=10, lavorato=0, gaser=0, ossidato=0)
    after = dict(before, in_lavorazione=0, cq=10)
    assert service.scadenza_cliente('2026-12-05', 'OSS', before, cfg, date(2026, 11, 25)) == service.scadenza_cliente(
        '2026-12-05', 'OSS', after, cfg, date(2026, 11, 25))


def test_tank_menu_entry_is_replaced_by_return_link(app):
    policy = SimpleNamespace(can=lambda code: code in {'home', 'all_serbatoi'}, has_direct_admin_role=False)
    for endpoint in ('main.serbatoi_page', 'main.serbatoi_qualita', 'main.home'):
        with app.test_request_context('/serbatoi'):
            request.url_rule = SimpleNamespace(endpoint=endpoint)
            html = render_template('partials/_nav_offcanvas.j2', operator_policy=policy, policy=policy,
                                   operator_user=None, current_user=None,
                                   operator_url_for=lambda endpoint, **kwargs: '/' + endpoint)
        if endpoint != 'main.home':
            assert html.count('Torna alla produzione') == 1
            assert 'href="/main.serbatoi_page"' not in html
            assert 'M15 18l-6-6l6-6' in html
        else:
            assert 'href="/main.serbatoi_page"' in html
