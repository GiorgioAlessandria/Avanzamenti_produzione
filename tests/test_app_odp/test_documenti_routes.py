from types import SimpleNamespace

import pytest
from flask import Flask

from app_odp.routes_modules import documenti as routes


@pytest.fixture()
def app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    return app


def _call_api(app, payload):
    with app.test_request_context(
        "/api/materiali/ricerca-articolo",
        method="POST",
        json=payload,
    ):
        return routes.api_ricerca_articolo.__wrapped__()


def test_api_ricerca_articolo_rejects_missing_code(app):
    response, status = _call_api(app, {})

    assert status == 400
    assert response.get_json() == {
        "ok": False,
        "error": "CodArt obbligatorio.",
    }


def test_api_ricerca_articolo_returns_not_found_contract(app, monkeypatch):
    monkeypatch.setattr(routes, "_find_articolo_lookup", lambda **_: None)

    response = _call_api(app, {"cod_art": "ART-404"})

    assert response.status_code == 200
    assert response.get_json() == {
        "ok": True,
        "found_component": False,
        "message": "Il codice inserito è errato oppure non è presente a gestionale.",
        "component": None,
        "image": {"found": False, "url": "", "file_name": ""},
        "orders": [],
        "orders_message": "",
    }


def test_api_ricerca_articolo_returns_component_image_and_orders_contract(
    app,
    monkeypatch,
):
    class Field:
        def __eq__(self, _other):
            return None

    class Select:
        def where(self, *_conditions):
            return self

    articolo = SimpleNamespace(
        CodArt="ART-1",
        VarianteArt="V1",
        IndiceModifica="R1",
        DesArt="Pompa",
        MagUM="PZ",
        TecniciUm="PZ",
    )
    monkeypatch.setattr(routes, "_find_articolo_lookup", lambda **_: articolo)
    monkeypatch.setattr(routes, "_find_materiale_image_path", lambda **_: None)
    monkeypatch.setattr(
        routes,
        "_build_articolo_ordini_attivi_rows",
        lambda **_: [{"ordine": "ORD-1"}],
    )
    monkeypatch.setattr(
        routes,
        "AcqGiacenze",
        SimpleNamespace(Giacenza=object(), CodArt=Field(), VarianteArt=Field()),
    )
    monkeypatch.setattr(
        routes,
        "func",
        SimpleNamespace(sum=lambda _value: None, coalesce=lambda *_values: None),
    )
    monkeypatch.setattr(routes, "select", lambda _value: Select())
    monkeypatch.setattr(
        routes,
        "db",
        SimpleNamespace(
            session=SimpleNamespace(
                execute=lambda _query: SimpleNamespace(scalar=lambda: 2.5)
            )
        ),
    )

    response = _call_api(app, {"cod_art": "art-1", "variante_art": "V1"})
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["found_component"] is True
    assert payload["component"]["CodArt"] == "ART-1"
    assert payload["component"]["VarianteArt"] == "V1"
    assert payload["component"]["IndiceModifica"] == "R1"
    assert payload["component"]["GiacenzaTotale"] == 2.5
    assert payload["image"] == {"found": False, "url": "", "file_name": ""}
    assert payload["orders"] == [{"ordine": "ORD-1"}]
    assert payload["orders_message"] == ""
