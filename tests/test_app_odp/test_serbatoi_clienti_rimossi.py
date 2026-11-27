import ast
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from openpyxl import load_workbook
from sqlalchemy import create_engine, inspect

from test_serbatoi_cliente_flow import app
from app_odp.models import db, AcqOrdineClienteAperto, InputOdp, ErpOutbox
from app_odp.services import serbatoi_service as s, serbatoi_cq_service as cq
from app_odp.routes_modules import serbatoi as routes


def detach_prepare():
    for lotto in s.Lotto.query.filter_by(cliente_id=1):
        lotto.stato, lotto.ubicazione = 'Lavorato', 'Bernardi'
    db.session.commit()


def test_unassigned_missing_customer_is_deleted_silently(app):
    count, notices = s._clienti_assenti_dal_gestionale({('C1', '1')})
    db.session.commit()
    assert count == 1 and notices == []
    assert db.session.get(s.Cliente, 2) is None
    assert s.Movimento.query.count() == 0


def test_assigned_missing_customer_is_detached_without_changing_production_or_quantities(app):
    detach_prepare()
    with db.engine.connect() as connection:
        connection.exec_driver_sql('PRAGMA foreign_keys=ON')
    before = [(l.id, l.quantita, l.stato, l.ubicazione) for l in s.Lotto.query.order_by(s.Lotto.id)]
    count, notices = s._clienti_assenti_dal_gestionale({('C2', '1')})
    db.session.commit()
    assert count == 1 and len(notices) == 1
    assert db.session.get(s.Cliente, 1) is None
    assert s.Impegno.query.count() == 0
    assert all(l.cliente_id is None for l in s.Lotto.query.all())
    assert before == [(l.id, l.quantita, l.stato, l.ubicazione) for l in s.Lotto.query.order_by(s.Lotto.id)]
    p = db.session.get(s.Produzione, 1)
    assert 'sganciato' in p.avviso_sgancio and 'CLIENTE-1' in p.avviso_sgancio
    assert p.anomalia == '' and p.quantita == 10
    assert [m.tipo for m in s.Movimento.query.all()] == ['SGANCIO_CLIENTE_ERP']
    assert ErpOutbox.query.count() == 0
    assert s._clienti_assenti_dal_gestionale({('C2', '1')}) == (0, [])


@pytest.mark.parametrize('state,quantity', [('Ossidato', 10), ('Spedito', 10), ('Ossidato', 0)])
def test_completed_history_is_archived_without_simulating_movements_even_when_lot_exhausted(app, state, quantity):
    detach_prepare()
    lotto = db.session.get(s.Lotto, 1)
    lotto.stato, lotto.quantita = state, quantity
    db.session.commit()
    count, notices = s._clienti_assenti_dal_gestionale({('C2', '1')})
    db.session.commit()
    assert count == 1 and notices == []
    assert db.session.get(s.Cliente, 1).archiviato
    assert lotto.cliente_id == 1 and lotto.stato == state and lotto.quantita == quantity
    assert s.Impegno.query.count() == 1 and s.Movimento.query.count() == 0
    assert s._clienti_assenti_dal_gestionale({('C2', '1')}) == (0, [])


@pytest.mark.parametrize('state,location,with_cq', [('Lavorato', 'Gaser', False), ('In controllo CQ', 'Bernardi', True)])
def test_pending_ddt_and_quality_are_kept_with_a_single_persistent_warning(app, state, location, with_cq):
    detach_prepare()
    lotto = db.session.get(s.Lotto, 1)
    lotto.stato, lotto.ubicazione = state, location
    if with_cq:
        db.session.add(cq.Controllo(fase='rientro', cliente_id=1, produzione_id=1, lotto_id=1, piano={}, quantita=10))
    db.session.commit()
    count, notices = s._clienti_assenti_dal_gestionale({('C2', '1')})
    db.session.commit()
    assert count == 1 and len(notices) == 1
    cliente = db.session.get(s.Cliente, 1)
    assert not cliente.archiviato and lotto.cliente_id == 1
    assert 'collegamenti conservati' in cliente.anomalia
    assert cliente.anomalia == db.session.get(s.Produzione, 1).avviso_sgancio
    assert s._clienti_assenti_dal_gestionale({('C2', '1')}) == (0, [])
    if with_cq:
        assert cq.Controllo.query.one().cliente_id == 1
    lotto.stato, lotto.ubicazione = 'Ossidato', 'Bernardi'
    db.session.commit()
    assert s._clienti_assenti_dal_gestionale({('C2', '1')}) == (1, [])
    db.session.commit()
    assert cliente.archiviato and cliente.anomalia == ''
    assert db.session.get(s.Produzione, 1).avviso_sgancio == ''


def test_order_still_in_source_is_not_deleted_even_if_its_article_is_no_longer_managed(app):
    for key in ('C1', 'C2'):
        db.session.add(AcqOrdineClienteAperto(IdDocumento=key, IdRigaDoc='1', CodArt='IGNORATO', QTA_SALDO_DOC='10'))
    db.session.commit()
    s.sincronizza(includi_barre=False)
    db.session.commit()
    assert s.Cliente.query.count() == 2
    assert not any(c.archiviato for c in s.Cliente.query.all())


def test_archived_order_is_readonly_searchable_and_exportable(app):
    s._clienti_assenti_dal_gestionale({('C2', '1')})
    db.session.commit()
    client = app.test_client()
    html = client.get('/serbatoi?tab=archivio_clienti&id=1&q=CLIENTE-1').get_data(as_text=True)
    assert 'Ordine archiviato' in html and 'CLIENTE-1' in html
    assert 'name="azione" value="avanza"' not in html
    assert 'name="azione" value="consuma"' not in html
    data = client.get('/serbatoi?tab=archivio_clienti&export=xlsx').data
    rows = list(load_workbook(BytesIO(data)).active.values)
    assert any(row[0] == 'CLIENTE-1' and row[6] == 'Controllato' for row in rows[1:])
    with pytest.raises(s.SerbatoiError, match='archiviato'):
        s.avanza_materiale(3, 1, 'spedisci', '123', 0, 'Tester')


def test_archived_orders_leave_open_flow_but_actual_shipments_remain_visible(app, monkeypatch):
    lotto = db.session.get(s.Lotto, 3)
    lotto.stato, lotto.ubicazione = 'Spedito', 'Cliente'
    s._clienti_assenti_dal_gestionale({('C2', '1')})
    db.session.commit()
    captured = {}
    def render(template, **values):
        captured.update(values)
        return 'OK'
    monkeypatch.setattr(routes, 'render_template', render)
    assert app.test_client().get('/serbatoi?tab=clienti').status_code == 200
    assert all(r['id'] != 1 for r in captured['rows'])
    assert captured['totali_fasi']['ossidato'] == 0
    assert any(r['id'] == 1 for r in captured['ordini_per_fase']['spedito'])


def test_detachment_warning_is_visible_in_production_list_and_does_not_block_new_assignment(app):
    detach_prepare()
    s._clienti_assenti_dal_gestionale({('C2', '1')})
    db.session.add(InputOdp(IdDocumento='P1', IdRiga='1', RifRegistraz='ODP-1', CodArt='LAV', Quantita='10', StatoOrdineErp='Pianificata'))
    db.session.commit()
    html = app.test_client().get('/serbatoi?tab=produzione&id=1').get_data(as_text=True)
    assert 'data-sganciato="true"' in html and 'Ordine sganciato' in html
    s.assegna_produzione(2, 1, 'Tester')
    assert db.session.get(s.Produzione, 1).avviso_sgancio == ''
    assert s.Impegno.query.filter_by(cliente_id=2, produzione_id=1).count() == 1


def test_schema_upgrade_is_additive_and_idempotent():
    source = Path(routes.__file__).resolve().parents[1] / 'app.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_ensure_serbatoi_schema')
    engine = create_engine('sqlite://')
    with engine.begin() as connection:
        for table in ('serbatoi_lotti', 'serbatoi_clienti_ordini', 'serbatoi_produzioni'):
            connection.exec_driver_sql(f'CREATE TABLE {table} (id INTEGER PRIMARY KEY)')
            connection.exec_driver_sql(f'INSERT INTO {table} (id) VALUES (1)')
    namespace = {'db': SimpleNamespace(engine=engine), 'inspect': inspect}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), 'exec'), namespace)
    namespace['_ensure_serbatoi_schema']()
    namespace['_ensure_serbatoi_schema']()
    with engine.connect() as connection:
        assert connection.exec_driver_sql('SELECT archiviato FROM serbatoi_clienti_ordini').scalar() == 0
        assert connection.exec_driver_sql('SELECT avviso_sgancio FROM serbatoi_produzioni').scalar() == ''


def test_returned_order_is_reactivated_and_only_its_absence_warning_is_removed(app):
    detach_prepare()
    lotto = db.session.get(s.Lotto, 1)
    lotto.ubicazione = 'Gaser'
    s._clienti_assenti_dal_gestionale({('C2', '1')})
    p = db.session.get(s.Produzione, 1)
    p.avviso_sgancio += '\nAltro avviso da mantenere'
    db.session.add_all([AcqOrdineClienteAperto(IdDocumento='C1', IdRigaDoc='1', CodArt='OSS', QTA_SALDO_DOC='10'),
                        AcqOrdineClienteAperto(IdDocumento='C2', IdRigaDoc='1', CodArt='OSS', QTA_SALDO_DOC='10')])
    db.session.commit()
    s.sincronizza(includi_barre=False)
    db.session.commit()
    assert db.session.get(s.Cliente, 1).anomalia == ''
    assert p.avviso_sgancio == 'Altro avviso da mantenere'
    db.session.get(s.Cliente, 1).archiviato = True
    db.session.commit()
    s.sincronizza(includi_barre=False)
    db.session.commit()
    assert not db.session.get(s.Cliente, 1).archiviato
