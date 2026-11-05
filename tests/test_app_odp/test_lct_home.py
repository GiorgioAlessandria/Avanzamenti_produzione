from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch

import pytest
from flask import Flask, render_template

from app_odp.filters import register_filters
from app_odp.services.lct_service import uses_lct_home, lct_home_rows, lct_sections, lct_fresa_busy
from app_odp.services.home_service import _render_fragments_for_home_config, _home_rows_for_config


def order(key, phase, state="Pianificata", owner="lct"):
    return NS(IdDocumento=key, IdRiga="1", NumProgrRiga="1", FaseAttiva=phase,
              StatoOrdine=state, CodArt=f"ART-{key}", DesArt=f"Articolo {key}",
              Quantita="10", QtyDaLavorare="10", IndiceModifica="A", VarianteArt="",
              PrioritaNumero=1, DataFineSched="", AttrezzaggioAttivo="0", RisorsaAttiva="LCT",
              CodTipoDoc="OP", LavorazioneAttiva="LCT", DistintaMateriale="[]",
              runtime_row=NS(Utente_operazione=owner, PrioritaInCarico=1))


@pytest.fixture
def ui():
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[2] / "app_odp/templates"))
    register_filters(app)
    app.jinja_env.globals["ordine_ref_display"] = lambda o: o.IdDocumento
    policy = NS(direct_assigned_roles=[NS(name="operatore_officina_LCT")],
                user=NS(username="lct"), can=lambda perm: True)
    app.context_processor(lambda: {"policy": policy})
    with app.test_request_context():
        yield app, policy


def group_rows():
    fresa, buratto = order("F1", "1", "Attivo"), order("B1", "2", "Attivo")
    detail = [{"id_documento": o.IdDocumento, "id_riga": "1", "fase": o.FaseAttiva} for o in (fresa, buratto)]
    group = NS(IsWorkGroup=True, WorkGroupOrdini=[fresa, buratto],
               WorkGroupMembers=[NS(IdDocumento="F1", IdRiga="1", TimeShareMode="FULL"),
                                 NS(IdDocumento="B1", IdRiga="1", TimeShareMode="ZERO")],
               WorkGroupUid="MASK-1", WorkGroupDisplayType="MASCHERATO", OrdineVisuale="F1 + B1",
               DettaglioGruppoVisuale=detail, IdDocumento="GROUP:MASK-1", IdRiga="GROUP")
    return group, fresa, buratto


def test_role_and_renderer_scope(ui):
    _, policy = ui
    assert uses_lct_home(policy, NS(renderer="standard"))
    assert not uses_lct_home(policy, NS(renderer="montaggio"))
    assert not uses_lct_home(policy, None)
    policy.direct_assigned_roles = [NS(name="operatore_officina")]
    assert not uses_lct_home(policy, NS(renderer="standard"))


def test_group_members_are_separate_rows_with_original_action_identity():
    group, fresa, buratto = group_rows()
    single = order("B2", "2", "In Sospeso")
    rows = lct_home_rows([group, single])
    assert rows == [fresa, buratto, single]
    assert fresa.LctMode == "Principale" and buratto.LctMode == "Mascherato"
    assert buratto.LctGroup is group
    assert group.WorkGroupMembers[1].TimeShareMode == "ZERO"
    sections = lct_sections(rows + [order("F2", "01"), order("B3", "2.0"), order("C", "2", "Chiusa")])
    assert [o.IdDocumento for o in sections["fresa_da_eseguire"]] == ["F2"]
    assert sections["fresa_in_corso"] == [fresa]
    assert sections["buratto_in_corso"] == [buratto, single]
    assert [o.IdDocumento for o in sections["buratto_da_eseguire"]] == ["B3"]
    assert lct_fresa_busy(rows, "lct")
    assert not lct_fresa_busy(rows, "other")
    # Dopo lo scioglimento, anche un oggetto già usato nella stessa sessione perde il gruppo grafico.
    assert lct_home_rows([buratto])[0].LctGroup is None
    assert buratto.LctMode == "Singolo"


def test_four_panels_render_existing_group_commands_and_escape_content(ui):
    _, policy = ui
    group, fresa, buratto = group_rows()
    rows = lct_home_rows([group]) + [order("F2", "1"), order("B2", "2", "In Sospeso")]
    rows[-1].DesArt = '<script>alert("x")</script>'
    html = render_template("partials/_home_standard.j2", lct_home=True, odp=rows,
                           lct_sections=lct_sections(rows), lct_fresa_busy=True,
                           metodo_montaggio_lookup={})
    assert html.count('class="lct-panel ') == 4
    assert 'data-row-key="F1|1"' in html and 'data-row-key="B1|1"' in html
    assert html.count('data-iddocumento="GROUP:MASK-1"') == 2
    assert 'data-use-selected="#lct_buratto_in_corso"' in html
    assert "Sospendi gruppo" in html and "Chiusure gruppo" in html
    assert "Mascherato" in html and "Principale" in html
    assert 'disabled title="Sospendi o termina la fresatura attiva' in html
    assert "&lt;script&gt;" in html and '<script>alert("x")</script>' not in html
    # Le finestre di quantità/lotti e i relativi endpoint rimangono quelli esistenti.
    assert 'id="toast-4"' in html and '"/api/ordini/chiudi"' in html


def test_live_fragments_target_all_four_tables_and_standard_role_is_unchanged(ui):
    _, policy = ui
    config = NS(renderer="standard")
    with patch("app_odp.services.home_service.active_policy", return_value=policy), \
         patch("app_odp.services.documenti_service._build_metodo_montaggio_lookup", return_value={}):
        fragments = _render_fragments_for_home_config(config, [order("B1", "2")])
        assert set(fragments) == {f"tbody_lct_{key}" for key in lct_sections([])}
        assert "B1" in fragments["tbody_lct_buratto_da_eseguire"]
        assert "Nessun ordine" in fragments["tbody_lct_fresa_in_corso"]
        policy.direct_assigned_roles = [NS(name="operatore_officina")]
        fragments = _render_fragments_for_home_config(config, [])
        assert set(fragments) == {"tbody_ordini_da_eseguire", "tbody_ordini_in_corso"}


def test_other_roles_keep_standard_layout_and_original_order_ids(ui):
    row = order("B1", "2", "Attivo")
    html = render_template("partials/_home_standard.j2", odp=[row], metodo_montaggio_lookup={})
    assert 'id="ordini_da_eseguire"' in html and 'id="ordini_in_corso"' in html
    assert 'id="home-lct"' not in html
    assert 'data-iddocumento="B1"' in html and 'data-is-work-group="1"' not in html


def test_home_expansion_is_only_for_lct_without_writes(ui):
    _, policy = ui
    group, fresa, buratto = group_rows()
    query = NS(all=lambda: [fresa, buratto])
    with patch("app_odp.services.home_service._query_for_home_config", return_value=query), \
         patch("app_odp.services.ordini_gruppi_service._collapse_work_group_rows_for_home", return_value=[group]):
        assert _home_rows_for_config(policy, NS(renderer="standard"), apply_priorita=False) == [fresa, buratto]
        policy.direct_assigned_roles = [NS(name="operatore_officina")]
        assert _home_rows_for_config(policy, NS(renderer="standard"), apply_priorita=False) == [group]
