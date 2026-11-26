import pytest

from test_serbatoi_cliente_flow import app, post
from app_odp.models import db
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as service


def test_before_working_coverage_and_origins_remain_editable_and_ddt_is_hidden(app):
    for lotto in service.Lotto.query.all():
        lotto.stato = 'Grezzo'
        lotto.ubicazione = 'Bernardi'
    db.session.get(service.Impegno, 1).realizzata = 0
    db.session.commit()
    html = app.test_client().get('/serbatoi?tab=clienti&id=1').get_data(as_text=True)
    assert 'id="serbatoi-cliente-ddt"' not in html
    assert 'name="azione" value="libera_impegno"' in html
    assert 'name="azione" value="consuma"' in html
    assert not service.cliente_lavorato(db.session.get(service.Cliente, 1))


@pytest.mark.parametrize('action, values', [
    ('assegna_odp', dict(cliente_id='1', produzione_id='1')),
    ('assegna_magazzino', dict(cliente_id='1', lotto_id='1')),
    ('libera_impegno', dict(impegno_id='1')),
    ('libera_materiale', dict(lotto_id='1')),
    ('riconcilia_cliente', dict(cliente_id='1')),
    ('consuma', dict(produzione_id='1')),
    ('consuma_da_spezzoni', dict(produzione_id='1')),
    ('libera_lotto_spezzoni', dict(produzione_id='1')),
    ('riconcilia', dict(produzione_id='1')),
])
def test_server_blocks_coverage_and_origin_changes_regardless_of_page(app, action, values):
    with pytest.raises(service.SerbatoiError, match='lavorato'):
        routes._verifica_sezioni_modificabili(dict(tab='produzione', **values), action)


def test_manual_post_cannot_release_worked_material(app):
    client, response = post(app, action='libera_materiale', tab='storico')
    assert response.status_code == 302
    assert db.session.get(service.Lotto, 1).quantita == 10
    assert service.Movimento.query.count() == 0
    with client.session_transaction() as session:
        assert any('sola lettura' in message for _, message in session['_flashes'])


@pytest.mark.parametrize('state', ['Lavorato', 'In controllo CQ', 'Ossidato', 'Spedito'])
def test_lock_persists_for_exhausted_lots_and_later_stages(app, state):
    for lotto in service.Lotto.query.all():
        lotto.stato = state
        lotto.quantita = 0
    db.session.commit()
    assert service.cliente_lavorato(db.session.get(service.Cliente, 1))
    assert service.produzione_cliente_lavorato(db.session.get(service.Produzione, 1))
    html = app.test_client().get('/serbatoi?tab=clienti&id=1').get_data(as_text=True)
    assert 'id="serbatoi-cliente-ddt"' in html
    assert 'name="azione" value="assegna_odp"' not in html
    assert 'name="azione" value="assegna_magazzino"' not in html
    assert 'name="azione" value="consuma"' not in html
    assert 'name="azione" value="libera_impegno"' not in html
