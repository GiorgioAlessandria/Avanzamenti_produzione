import hashlib
from io import BytesIO
import json
from types import SimpleNamespace

import pytest
from flask import render_template, request
from pypdf import PdfReader

from test_serbatoi_cliente_flow import app, post
from app_odp.models import db
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as service


def plan(count):
    return dict(codice='LAV', revisione='1', controlli=[dict(
        descrizione=f'Controllo {i + 1}: verifica delle superfici, delle quote del serbatoio e della posizione del foro laterale.',
        strumento='calibro', frequenza='100%', valore='vedi disegno', campione=None,
    ) for i in range(count)])


def complete_controls():
    returned = service.sposta(db.session.get(service.Lotto, 2), 10, routes.cq.STATO_CQ,
                              'Bernardi', 'Tester', 'RIENTRO', cliente_id=1, ddt='123')
    service.sposta(returned, 10, 'Ossidato', 'Bernardi', 'Tester', 'CQ_OK', cliente_id=1)
    rows = []
    for phase, count in [('fresatura', 7), ('rientro', 6)]:
        control = routes.cq.Controllo(
            fase=phase, produzione_id=1, cliente_id=1 if phase == 'rientro' else None,
            lotto_id=returned.id if phase == 'rientro' else None, quantita=10, ko=0,
            piano=plan(count), risultati=[dict(verificati=10, ko=0, note='') for _ in range(count)],
            operatore='Tester', data='2026-11-25T10:00:00+01:00')
        db.session.add(control)
        rows.append(control)
    db.session.commit()
    return rows


def test_cqc_button_next_to_shipping_keeps_issuance_checks_and_pdf_has_one_page(app):
    client = app.test_client()
    html = client.get('/serbatoi?tab=clienti&id=1').get_data(as_text=True)
    assert '>Spedisci al cliente</button>' in html and '>Genera CQC</a>' in html
    assert client.get('/serbatoi/cqc/1').status_code == 302
    complete_controls()
    response = client.get('/serbatoi/cqc/1')
    assert response.status_code == 200 and response.mimetype == 'application/pdf'
    pdf = PdfReader(BytesIO(response.data))
    assert len(pdf.pages) == 1
    text = pdf.pages[0].extract_text()
    assert 'Fresatura' in text and 'Ossidatura' in text and 'Articolo cliente OSS' in text
    assert text.count('Controllo 7:') == 1 and text.count('Controllo 6:') == 2


def test_quality_display_uses_oxidized_code_and_plan_identity_is_unchanged(app):
    control = complete_controls()[1]
    client = app.test_client()
    html = client.get(f'/serbatoi/qualita?id={control.id}').get_data(as_text=True)
    assert '>OSS · ' in html
    assert 'Serbatoio ossidato: OSS' in html
    db.session.add(routes.cq.Piano(codice='LAV', revisione='1', controlli=plan(1)['controlli']))
    db.session.commit()
    result = routes.cq.piano_codice('LAV')
    assert result['codice_oss'] == 'OSS'
    assert result['codice'] == 'LAV'
    identity = dict(codice='LAV', revisione='1', controlli=plan(1)['controlli'])
    assert result['versione'] == hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


@pytest.mark.parametrize('endpoint', ['main.serbatoi_page', 'main.serbatoi_qualita'])
@pytest.mark.parametrize('allowed', [True, False])
def test_return_to_production_is_visible_in_tank_pages_when_authorized(app, endpoint, allowed):
    policy = SimpleNamespace(can=lambda code: code == 'home' and allowed, has_direct_admin_role=False)
    with app.test_request_context('/serbatoi'):
        request.url_rule = SimpleNamespace(endpoint=endpoint)
        html = render_template('partials/_nav_offcanvas.j2', operator_policy=policy, policy=policy,
                               operator_user=None, current_user=None,
                               operator_url_for=lambda endpoint, **kwargs: '/produzione' if endpoint == 'main.home' else '/other')
    assert ('Torna alla produzione' in html) == allowed
