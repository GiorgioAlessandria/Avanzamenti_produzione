from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment

from test_serbatoi_cliente_flow import app
from app_odp.models import db
from app_odp.routes_modules import home
from app_odp.services import serbatoi_cq_service as cq
from app_odp.services import serbatoi_service as service


def pending_control(lot_id=2):
    lot = db.session.get(service.Lotto, lot_id)
    lot.stato, lot.ubicazione = cq.STATO_CQ, 'Bernardi'
    control = cq.Controllo(fase='rientro', cliente_id=1, produzione_id=1, lotto_id=lot.id,
                           piano={}, quantita=lot.quantita)
    db.session.add(control)
    db.session.commit()
    return control


def test_counter_counts_only_pending_returns_with_available_material(app):
    first = pending_control(1)
    second = pending_control(2)
    db.session.add(cq.Controllo(fase='fresatura', produzione_id=1, piano={}, quantita=10))
    db.session.commit()
    assert cq.conteggio_controlli_da_eseguire() == 2
    first.data = '2026-11-25T10:00:00+01:00'
    db.session.get(service.Lotto, 1).stato = 'Ossidato'
    db.session.get(service.Lotto, 2).quantita = 0
    db.session.commit()
    assert cq.conteggio_controlli_da_eseguire() == 0


def test_unauthorized_user_does_not_query_or_see_quality_counts(monkeypatch):
    def unexpected_query():
        raise AssertionError('Unexpected count query')
    monkeypatch.setattr(home, 'conteggio_controlli_da_eseguire', unexpected_query)
    assert home._controlli_qualita_da_eseguire(SimpleNamespace(can=lambda code: False)) == 0


def test_admin_preview_receives_pending_quality_count(app, monkeypatch):
    pending_control()
    policy = SimpleNamespace(has_direct_admin_role=True, can=lambda code: code == 'all_serbatoi')
    monkeypatch.setattr(home, 'active_policy', lambda: policy)
    monkeypatch.setattr(home, 'active_user', lambda: SimpleNamespace(username='Tester'))
    monkeypatch.setattr(home, 'active_token', lambda: None)
    monkeypatch.setattr(home, '_allowed_home_reparto_configs', lambda policy: [])
    monkeypatch.setattr(home, 'render_template', lambda template, **context: context)
    with app.test_request_context('/?vista=lct'):
        result = home.home.__wrapped__()
    assert result['lct_home'] is True
    assert result['controlli_qualita_da_eseguire'] == 1


@pytest.mark.parametrize('is_lct', [True, False])
def test_operational_home_receives_notice_only_in_lct_mode(app, monkeypatch, is_lct):
    from app_odp.services import lct_service
    pending_control()
    policy = SimpleNamespace(has_direct_admin_role=False, can=lambda code: code == 'produzione_serbatoi')
    config = SimpleNamespace(tab_code='officina', renderer='standard', template='partials/_home_lct.j2')
    monkeypatch.setattr(home, 'active_policy', lambda: policy)
    monkeypatch.setattr(home, 'active_user', lambda: SimpleNamespace(username='Tester', preferences={}))
    monkeypatch.setattr(home, 'active_token', lambda: None)
    monkeypatch.setattr(home, '_first_allowed_home_reparto_config', lambda policy: config)
    monkeypatch.setattr(home, '_policy_can_access_home_config', lambda *args: True)
    monkeypatch.setattr(home, '_home_rows_for_config', lambda *args, **kwargs: [])
    monkeypatch.setattr(home, '_home_method_settings_for_user', lambda *args: dict(path_key='', prefisso='', tipo=''))
    monkeypatch.setattr(home, '_home_ui_texts_for_user', lambda *args: {})
    monkeypatch.setattr(home, '_build_metodo_lookup', lambda *args, **kwargs: {})
    monkeypatch.setattr(home, '_last_log_token', lambda: '')
    monkeypatch.setattr(lct_service, 'uses_lct_home', lambda *args: is_lct)
    monkeypatch.setattr(lct_service, 'lct_sections', lambda *args: {})
    monkeypatch.setattr(lct_service, 'lct_fresa_busy', lambda *args: False)
    monkeypatch.setattr(home, 'render_template', lambda template, **context: context)
    with app.test_request_context('/'):
        result = home.home.__wrapped__()
    assert result['controlli_qualita_da_eseguire'] == (1 if is_lct else 0)


@pytest.mark.parametrize('count, lct, expected', [(0, True, False), (2, True, True), (1, False, False)])
def test_warning_visibility_and_quality_link(count, lct, expected):
    source = (Path(home.__file__).resolve().parents[1] / 'templates/base.j2').read_text(encoding='utf-8')
    environment = Environment()
    environment.parse(source)
    start = source.index('{% if lct_home|default(false) and controlli_qualita_da_eseguire|default(0) %}')
    end = source.index('{% endif %}', start) + len('{% endif %}')
    rendered = environment.from_string(source[start:end]).render(
        lct_home=lct, controlli_qualita_da_eseguire=count,
        operator_url_for=lambda endpoint: '/serbatoi/qualita?tab_session=test',
    )
    assert ('serbatoi-cq-avviso-lct' in rendered) == expected
    if expected:
        assert '2 controlli' in rendered
        assert '/serbatoi/qualita?tab_session=test' in rendered
