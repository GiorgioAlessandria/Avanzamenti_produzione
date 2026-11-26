from types import SimpleNamespace

import pytest

from test_serbatoi_cliente_flow import app
from app_odp.models import db
from app_odp.routes_modules import serbatoi as routes
from app_odp.services import serbatoi_service as service


@pytest.mark.parametrize('value, expected', [
    ('2026-12-05', '05/12/2026'),
    ('2026-12-05T10:00:00', '05/12/2026'),
    ('05/12/2026', '05/12/2026'),
    ('', 'non indicata'),
    (None, 'non indicata'),
])
def test_customer_reference_contains_row_and_delivery_date(value, expected):
    customer = SimpleNamespace(riferimento='OC-100', id_riga='3', consegna=value)
    assert service.riferimento_cliente(customer) == f'OC-100 · riga 3 · Consegna {expected}'


def test_assigned_customer_delivery_is_visible_in_odp_warehouse_and_shipments(app, monkeypatch):
    customer = db.session.get(service.Cliente, 1)
    customer.consegna = '2026-12-05'
    db.session.commit()
    monkeypatch.setattr(routes, 'active_policy', lambda: SimpleNamespace(can=lambda code: True, has_direct_admin_role=False))
    client = app.test_client()
    label = 'CLIENTE-1 · riga 1 · Consegna 05/12/2026'
    for tab in ('produzione', 'magazzino', 'storico', 'clienti'):
        response = client.get(f'/serbatoi?tab={tab}&id=1')
        assert response.status_code == 200
        assert label in response.get_data(as_text=True)
