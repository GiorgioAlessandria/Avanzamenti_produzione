from datetime import date
from types import SimpleNamespace

import pytest
from flask import Flask

from app_odp.models import Famiglia, InputOdp, db
from app_odp.services.vendite_raggruppamenti_service import (
    build_machine_grouping, save_machine_group, delete_machine_group,
)
from app_odp.services.vendite_assegnazioni_service import (
    VenditeAssegnazioniError, VenditeAssegnazioniConflictError,
)
from app_odp.services.vendite_service import _build_vendite_payload, _customer_assignments
from app_odp.vendite_models import VenditeOrdineCliente, VenditeOrdineClienteRiga, VenditeRaggruppamento


@pytest.fixture()
def app():
    app = Flask(__name__)
    app.config.update(TESTING=True, SQLALCHEMY_DATABASE_URI="sqlite://")
    db.init_app(app)
    with app.app_context():
        db.create_all(bind_key=None)
        db.session.add_all([
            Famiglia(Codice="F1", Descrizione="Grandi"),
            Famiglia(Codice="F2", Descrizione="Piccole"),
            InputOdp(IdDocumento="DOC", IdRiga="1", CodFamiglia="F3"),
        ])
        db.session.commit()
    yield app
    with app.app_context():
        db.session.remove()
        db.drop_all(bind_key=None)


def test_group_crud_catalogue_and_concurrent_updates(app):
    with app.app_context():
        group = save_machine_group({"name": " Macchine grandi ", "family_codes": ["F1", "F3", "F1"]})
        group_id, version = group.id, group.versione
        db.session.commit()
        db.session.remove()
        db.create_all(bind_key=None)  # Riavvio: la nuova tabella non perde la configurazione.
        config = build_machine_grouping()
        assert config["groups"] == [{"id": group_id, "version": version, "name": "Macchine grandi",
                                     "family_codes": ["F1", "F3"]}]
        assert {f["code"] for f in config["families"]} == {"F1", "F2", "F3"}
        with pytest.raises(VenditeAssegnazioniConflictError):
            save_machine_group({"name": "MACCHINE GRANDI", "family_codes": ["F2"]})
        # Le famiglie possono sovrapporsi: la vista unica elimina i duplicati.
        save_machine_group({"name": "Altro", "family_codes": ["F1"]})
        save_machine_group({"id": group_id, "version": version, "name": "Grandi", "family_codes": ["F2"]})
        with pytest.raises(VenditeAssegnazioniConflictError):
            delete_machine_group(group_id, {"version": version})
        group = db.session.get(VenditeRaggruppamento, group_id)
        delete_machine_group(group_id, {"version": group.versione})
        db.session.commit()
        assert [g["name"] for g in build_machine_grouping()["groups"]] == ["Altro"]
        assert InputOdp.query.count() == 1
        assert Famiglia.query.count() == 2
        with pytest.raises(VenditeAssegnazioniConflictError):
            delete_machine_group(group_id, {"version": version})


@pytest.mark.parametrize("payload", [
    None, {}, {"name": "", "family_codes": ["F1"]},
    {"name": "x" * 81, "family_codes": ["F1"]},
    {"name": "A", "family_codes": []}, {"name": "A", "family_codes": "F1"},
    {"name": "A", "family_codes": [None]}, {"name": "A", "family_codes": ["SCONOSCIUTA"]},
    {"name": "A", "family_codes": ["F1"], "id": True},
])
def test_invalid_groups_are_rejected(app, payload):
    with app.app_context():
        with pytest.raises(VenditeAssegnazioniError):
            save_machine_group(payload)
        assert VenditeRaggruppamento.query.count() == 0


def test_group_api_permissions_conflicts_and_shared_reading(app, monkeypatch):
    from app_odp.routes_modules import vendite
    from app_odp.policy import decorator
    from app_odp.routes_blueprint import main_bp

    permissions = {"vendite"}
    policy = SimpleNamespace(can=lambda code: code in permissions)
    monkeypatch.setattr(decorator, "resolve_operator_session", lambda: object())
    monkeypatch.setattr(decorator, "active_policy", lambda: policy)
    monkeypatch.setattr(vendite, "active_policy", lambda: policy)
    monkeypatch.setattr(vendite, "build_vendite_payload", lambda **kwargs: {"machines": []})
    app.register_blueprint(main_bp)
    client = app.test_client()
    payload = {"name": "Grandi", "family_codes": ["F1"]}
    assert client.post("/api/vendite/raggruppamenti", json=payload).status_code == 403
    permissions.add("carica_ordini_cliente")
    response = client.post("/api/vendite/raggruppamenti", json=payload)
    assert response.status_code == 200
    group = response.json["data"]["groups"][0]
    assert client.post("/api/vendite/raggruppamenti", json=payload).status_code == 409
    assert client.post("/api/vendite/raggruppamenti", json=[]).status_code == 400
    permissions.remove("carica_ordini_cliente")
    response = client.get("/api/vendite/ordini-macchina")
    assert response.status_code == 200
    assert response.json["data"]["grouping"]["groups"] == [group]
    url = f"/api/vendite/raggruppamenti/{group['id']}"
    assert client.delete(url, json={"version": group["version"]}).status_code == 403
    permissions.add("carica_ordini_cliente")
    assert client.delete(url, json={"version": 0}).status_code == 409
    assert client.delete(url, json={"version": group["version"]}).status_code == 200
    permissions.remove("vendite")
    assert client.post("/api/vendite/raggruppamenti", json=payload).status_code == 403


def test_shipping_date_is_the_assigned_customer_row_date(app):
    with app.app_context():
        order = VenditeOrdineCliente(
            cliente_nome="Cliente", cliente_chiave="cliente", numero_ordine="OC1",
            numero_ordine_chiave="oc1", creato_da_nome="operatore",
            data_spedizione=date(2026, 10, 1),
        )
        row = VenditeOrdineClienteRiga(
            ordine_cliente=order, posizione=1, modello_codice="M1", modello_variante="",
            data_consegna=date(2026, 10, 5), odp_id_documento="DOC", odp_id_riga="1",
            odp_matricola="MAT1",
        )
        db.session.add(order)
        db.session.flush()
        machines = [
            SimpleNamespace(IdDocumento="DOC", IdRiga="1", CodMatricola="MAT1", CodFamiglia=" F1 ", CodArt="M1"),
            SimpleNamespace(IdDocumento="DOC", IdRiga="2", CodMatricola="MAT2", CodFamiglia="F1", CodArt="M2"),
        ]
        data = _build_vendite_payload(machines, customer_assignments=_customer_assignments(machines))
        assert data["machines"][0]["shipping_date"] == "2026-10-05"
        assert data["machines"][0]["customer_order"] == "OC1"
        assert data["machines"][1]["shipping_date"] == ""
        assert all(m["family_code"] == "F1" for m in data["machines"])
        row.data_consegna = date(2026, 11, 20)
        db.session.flush()
        assert _customer_assignments(machines)[("serial", "mat1")]["shipping_date"] == "2026-11-20"
        row.odp_id_documento = row.odp_id_riga = row.odp_matricola = None
        db.session.flush()
        assert _customer_assignments(machines) == {}
