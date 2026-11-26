from decimal import Decimal
from types import SimpleNamespace

import pytest

from test_serbatoi_cliente_flow import app, post
from app_odp.models import db
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as service


@pytest.fixture
def barre(app):
    cfg = service.parametri()
    cfg.codice_barre, cfg.fornitore_barre_tipo, cfg.fornitore_barre_codice = 'BARRA', 'F', '100'
    db.session.add(cfg)
    b = service.Barre(fornitore_tipo='F', fornitore_codice='100', fornitore_nome='Fornitore',
                      riferimento='BARRE-1', codice='BARRA', lunghezza_mm=6000, quantita=20)
    db.session.add(b)
    db.session.flush()
    for index, quantity, remainder, initial in ((1, 1, 3000, 10), (2, 0, 1500, 4)):
        db.session.add(service.Lotto(riferimento=f'BAR-{index}', tipo='Barra', codice='BARRA',
                                     barre_id=b.id, quantita=quantity, residuo_barra_mm=remainder,
                                     iniziale=initial, stato='Disponibile', ubicazione='Bernardi'))
    db.session.commit()
    return b


def test_exhaustion_excludes_estimated_remainder_and_preserves_trace(app, barre):
    lots = service.Lotto.query.filter_by(barre_id=barre.id).order_by(service.Lotto.id).all()
    before = [(lot.quantita, lot.residuo_barra_mm) for lot in lots]
    assert routes._riga_barre(barre, lots)['residue'] == Decimal('1.75')
    assert service.segna_barre_esaurite(barre.id, 'Tester') == 10500
    db.session.flush()
    assert [(lot.quantita, lot.residuo_barra_mm) for lot in lots] == before
    assert all(lot.stato == 'Esaurito' for lot in lots)
    row = routes._riga_barre(barre, lots)
    assert row['residue'] == 0
    assert row['residuo_escluso'] == Decimal('1.75')
    assert row['consumate'] == Decimal('12.25')
    assert 'Esaurito' in row['stato']
    assert barre.quantita == 20
    movements = service.Movimento.query.filter_by(tipo='ESAURIMENTO_BARRE_MM').all()
    assert sum(m.quantita for m in movements) == 10500
    assert all(m.operatore == 'Tester' for m in movements)
    service.segna_barre_esaurite(barre.id, 'Tester')
    db.session.flush()
    assert service.Movimento.query.filter_by(tipo='ESAURIMENTO_BARRE_MM').count() == 2


def test_exhausted_bar_lots_are_not_proposed_for_cutting(app, barre, monkeypatch):
    monkeypatch.setattr(routes, 'active_policy', lambda: SimpleNamespace(can=lambda code: True, has_direct_admin_role=False))
    service.segna_barre_esaurite(barre.id, 'Tester')
    db.session.commit()
    response = app.test_client().get(f'/serbatoi?tab=barre&id={barre.id}')
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'Residuo teorico escluso dalla disponibilità: 1,75' in html
    assert '>BAR-1' not in html and '>BAR-2' not in html
    assert 'name="azione" value="esaurisci_barre"' not in html
    assert not service.Lotto.query.filter_by(barre_id=barre.id, stato='Disponibile').first()


def test_exhaustion_requires_existing_purchase_permission_and_switch(app, barre, monkeypatch):
    _, response = post(app, action='esaurisci_barre', tab='barre', id=str(barre.id), barre_id=str(barre.id), esaurito='1')
    assert response.status_code == 403
    monkeypatch.setattr(routes, 'active_policy', lambda: SimpleNamespace(can=lambda code: True, has_direct_admin_role=False))
    _, response = post(app, action='esaurisci_barre', tab='barre', id=str(barre.id), barre_id=str(barre.id))
    assert response.status_code == 302
    assert service.Lotto.query.filter_by(barre_id=barre.id, stato='Disponibile').count() == 2
    _, response = post(app, action='esaurisci_barre', tab='barre', id=str(barre.id), barre_id=str(barre.id), esaurito='1')
    assert response.status_code == 302
    assert service.Lotto.query.filter_by(barre_id=barre.id, stato='Disponibile').count() == 0


def test_new_actual_receipt_can_be_available_without_restoring_old_remainders(app, barre):
    service.segna_barre_esaurite(barre.id, 'Tester')
    received = service.ricevi_barre(barre.id, '2', 'DDT-NEW', 'Tester')
    db.session.flush()
    row = routes._riga_barre(barre, service.Lotto.query.filter_by(barre_id=barre.id).all())
    assert received.stato == 'Disponibile'
    assert row['residue'] == 2
    assert row['residuo_escluso'] == Decimal('1.75')


def test_customer_summary_starts_collapsed_and_exhaustion_command_is_available(app, barre, monkeypatch):
    client = app.test_client()
    html = client.get('/serbatoi?tab=clienti&id=1').get_data(as_text=True)
    assert '<details class="card mb-3" aria-labelledby="serbatoi-clienti-riepilogo">' in html
    assert '<summary class="card-header py-2 fw-semibold" id="serbatoi-clienti-riepilogo">' in html
    monkeypatch.setattr(routes, 'active_policy', lambda: SimpleNamespace(can=lambda code: True, has_direct_admin_role=False))
    html = client.get(f'/serbatoi?tab=barre&id={barre.id}').get_data(as_text=True)
    assert 'name="azione" value="esaurisci_barre"' in html
    assert 'Segna esaurito' in html
