import pytest

from test_serbatoi_cliente_flow import app, post
from app_odp.models import db, InputOdp
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as service


def prepare_worked(produced=288, open_order=False):
    service.Lotto.query.delete()
    p = db.session.get(service.Produzione, 1)
    p.quantita, p.avviata = 288, True
    c = db.session.get(service.Cliente, 1)
    c.quantita = c.saldo_erp = 288
    commitment = db.session.get(service.Impegno, 1)
    commitment.quantita, commitment.realizzata = 288, produced
    cfg = service.parametri()
    cfg.codici_spezzoni = ['SPEZ']
    db.session.add(cfg)
    material = service.Lotto(riferimento='LAVORATI', tipo='Materiale', codice='LAV',
                             articolo_id=p.articolo_id, produzione_id=p.id, cliente_id=c.id,
                             quantita=produced, iniziale=produced, stato='Lavorato', ubicazione='Bernardi')
    stock = service.Lotto(riferimento='SPEZZONI-ORIGINE', tipo='Spezzone', codice='SPEZ',
                          quantita=500, iniziale=500, stato='Disponibile', ubicazione='Bernardi')
    db.session.add_all([material, stock])
    db.session.flush()
    service.movimento('COMPLETAMENTO_LCT', produced, 'Tester', destinazione=material, produzione=p)
    if open_order:
        db.session.add(InputOdp(IdDocumento='P1', IdRiga='1', CodArt='LAV', Quantita='288',
                                NumFase='[1,2]', StatoOrdineErp='Attivo'))
    db.session.commit()
    return p, stock, material


@pytest.mark.parametrize('already_traced', [0, 60])
def test_completed_odp_can_complete_only_missing_spezzoni(app, already_traced):
    p, stock, material = prepare_worked()
    previous = None
    if already_traced:
        service._registra_consumo_spezzoni(p, stock, already_traced, 'Operatore precedente')
        db.session.commit()
        previous = service.Consumo.query.one()
    assert service.spezzoni_mancanti_consuntivo(p) == 288 - already_traced
    client = app.test_client()
    html = client.get('/serbatoi?tab=produzione&id=1').get_data(as_text=True)
    assert 'Completa provenienza spezzoni' in html
    assert 'name="azione" value="consuma"' in html
    assert 'name="azione" value="libera_lotto_spezzoni"' not in html
    _, response = post(app, action='consuma', tab='produzione', produzione_id=str(p.id), lotto_id=str(stock.id))
    assert response.status_code == 302
    assert service._spezzoni_prelevati(p) == 288
    assert stock.quantita == 212
    assert material.quantita == 288
    assert db.session.get(service.ImpegnoSpezzone, p.id) is None
    if previous:
        assert db.session.get(service.Consumo, previous.id).quantita == 60
    assert not service.spezzoni_assegnabili(p)
    html = client.get('/serbatoi?tab=produzione&id=1').get_data(as_text=True)
    assert 'name="azione" value="consuma"' not in html
    with pytest.raises(service.SerbatoiError):
        routes._verifica_sezioni_modificabili(dict(produzione_id=str(p.id)), 'consuma')


def test_partial_working_order_completes_past_trace_and_reserves_remaining_pieces(app):
    p, stock, material = prepare_worked(produced=100, open_order=True)
    service.impegna_spezzoni(p.id, stock.id, 'Tester')
    db.session.flush()
    assert service._spezzoni_prelevati(p) == 100
    assert stock.quantita == 400
    assert db.session.get(service.ImpegnoSpezzone, p.id).quantita == 188
    assert material.quantita == 100
    assert not service.spezzoni_assegnabili(p)
