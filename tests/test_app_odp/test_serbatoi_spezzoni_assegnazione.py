import pytest
from types import SimpleNamespace

from test_serbatoi_cliente_flow import app, post
from app_odp.models import db, InputOdp
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as service


def prepare(state='Attivo'):
    for lotto in service.Lotto.query.all():
        lotto.stato = 'In lavorazione'
        lotto.ubicazione = 'Bernardi'
    db.session.get(service.Impegno, 1).realizzata = 0
    p = db.session.get(service.Produzione, 1)
    p.avviata = state != 'Pianificata'
    cfg = service.parametri()
    cfg.codici_spezzoni = ['SPEZ']
    db.session.add(cfg)
    db.session.add(InputOdp(IdDocumento='P1', IdRiga='1', RifRegistraz='ODP-1', CodArt='LAV',
                            Quantita='10', NumFase='[1,2]', CodLavorazione='["FRESA","BURATTO"]',
                            StatoOrdineErp=state))
    stock = service.Lotto(riferimento='SPEZZONI', tipo='Spezzone', codice='SPEZ', quantita=100,
                          iniziale=100, stato='Disponibile', ubicazione='Bernardi')
    db.session.add(stock)
    db.session.commit()
    return p, stock


@pytest.mark.parametrize('state', ['Pianificata', 'Attivo', 'In Sospeso'])
@pytest.mark.parametrize('page', ['produzione', 'clienti'])
def test_customer_assignment_does_not_block_spezzoni_before_worked(app, state, page):
    p, stock = prepare(state)
    html = app.test_client().get(f'/serbatoi?tab={page}&id=1').get_data(as_text=True)
    assert 'name="azione" value="consuma"' in html
    _, response = post(app, action='consuma', tab=page, produzione_id=str(p.id), lotto_id=str(stock.id))
    assert response.status_code == 302
    commitment = db.session.get(service.ImpegnoSpezzone, p.id)
    assert commitment is not None and commitment.lotto_id == stock.id and commitment.quantita == 10


def test_other_worked_odp_for_same_customer_does_not_block_this_odp(app):
    p, stock = prepare()
    other = service.Produzione(id_documento='P2', id_riga='1', riferimento='ODP-2',
                               riferimento_stock='STOCK-2', articolo_id=p.articolo_id,
                               quantita=5, fase_inizio='1', fase_fine='2')
    db.session.add(other)
    db.session.flush()
    db.session.add(service.Lotto(riferimento='OTHER-WORKED', tipo='Materiale', codice='LAV',
                                 articolo_id=p.articolo_id, produzione_id=other.id, cliente_id=1,
                                 quantita=5, iniziale=5, stato='Lavorato', ubicazione='Bernardi'))
    db.session.commit()
    assert service.produzione_cliente_lavorato(p)
    assert not service.produzione_lavorata(p)
    service.impegna_spezzoni(p.id, stock.id, 'Tester')
    db.session.flush()
    assert db.session.get(service.ImpegnoSpezzone, p.id).lotto_id == stock.id


@pytest.mark.parametrize('assigned', [True, False])
def test_worked_odp_blocks_spezzoni_even_when_it_is_for_stock(app, assigned):
    p, stock = prepare()
    first = service.Lotto.query.filter_by(produzione_id=p.id).first()
    first.stato = 'Lavorato'
    if not assigned:
        service.Impegno.query.delete()
        for lotto in service.Lotto.query.filter_by(produzione_id=p.id):
            lotto.cliente_id = None
    db.session.commit()
    assert service.produzione_lavorata(p)
    html = app.test_client().get('/serbatoi?tab=produzione&id=1').get_data(as_text=True)
    assert 'name="azione" value="consuma"' not in html
    with pytest.raises(service.SerbatoiError, match='già lavorato'):
        service.impegna_spezzoni(p.id, stock.id, 'Tester')
    with pytest.raises(service.SerbatoiError, match='già lavorato'):
        routes._verifica_sezioni_modificabili(dict(produzione_id=str(p.id)), 'consuma')


def test_spezzoni_page_lists_assigned_odp_only_until_worked(app, monkeypatch):
    p, stock = prepare()
    monkeypatch.setattr(routes, 'active_policy', lambda: SimpleNamespace(can=lambda code: True, has_direct_admin_role=False))
    client = app.test_client()
    html = client.get(f'/serbatoi?tab=spezzoni&id={stock.id}').get_data(as_text=True)
    assert 'name="azione" value="consuma_da_spezzoni"' in html
    first = service.Lotto.query.filter_by(produzione_id=p.id).first()
    first.stato = 'Lavorato'
    db.session.commit()
    html = client.get(f'/serbatoi?tab=spezzoni&id={stock.id}').get_data(as_text=True)
    assert 'name="azione" value="consuma_da_spezzoni"' not in html
