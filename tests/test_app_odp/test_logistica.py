from datetime import date, datetime
from types import SimpleNamespace

import pytest
from flask import Flask
from sqlalchemy import inspect
from werkzeug.exceptions import Forbidden

from app_odp.logistica_models import (
    LOGISTICA_BIND_KEY,
    MovimentoLogistico,
    VettoreTrasporto,
)
from app_odp.models import db
from app_odp.policy import decorator as policy_decorator
from app_odp.routes_modules import logistica as logistica_routes
from app_odp.routes_modules.logistica import _later_date, _row_class


@pytest.fixture()
def app(tmp_path):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_BINDS={
            LOGISTICA_BIND_KEY: f"sqlite:///{tmp_path / 'logistica.sqlite'}",
        },
    )
    db.init_app(app)
    with app.app_context():
        db.create_all(bind_key=LOGISTICA_BIND_KEY)
    return app


def test_logistica_creates_two_tables_and_persists_a_movement(app):
    with app.app_context():
        assert set(inspect(db.engines[LOGISTICA_BIND_KEY]).get_table_names()) == {
            "movimenti",
            "vettori",
        }

        vettore = VettoreTrasporto(nome="Trasporti Rossi")
        db.session.add(vettore)
        db.session.flush()
        db.session.add(
            MovimentoLogistico(
                vettore_id=vettore.id,
                movimento="CARICO",
                tipologia="FORNITORE",
                controparte="Fornitore S.p.A.",
                data=date(2026, 7, 28),
                materiale="Acciaio",
                note="Ingresso nord",
                creato_da_nome="operatore",
            )
        )
        db.session.commit()

        saved = MovimentoLogistico.query.one()
        assert saved.vettore.nome == "Trasporti Rossi"
        assert saved.materiale == "Acciaio"
        assert saved.completato_il is None


@pytest.mark.parametrize(
    ("scheduled", "tipologia", "expected"),
    [
        (date(2026, 7, 26), "CLIENTE", "movimento-scaduto"),
        (date(2026, 7, 27), "CLIENTE", "movimento-cliente"),
        (date(2026, 7, 28), "FORNITORE", "movimento-fornitore"),
    ],
)
def test_row_color_gives_priority_to_overdue(scheduled, tipologia, expected):
    movimento = SimpleNamespace(data=scheduled, tipologia=tipologia)
    assert _row_class(movimento, date(2026, 7, 27)) == expected


def test_later_date_only_accepts_a_postponement():
    current = date(2026, 7, 27)

    assert _later_date(current, "2026-07-28") == date(2026, 7, 28)
    with pytest.raises(ValueError, match="successiva"):
        _later_date(current, "2026-07-27")
    with pytest.raises(ValueError, match="data valida"):
        _later_date(current, "domani")


def test_note_remains_editable_after_movement_completion(app, monkeypatch):
    with app.app_context():
        vettore = VettoreTrasporto(nome="Trasporti Rossi")
        movimento = MovimentoLogistico(
            vettore=vettore,
            movimento="SCARICO",
            tipologia="CLIENTE",
            controparte="Cliente S.p.A.",
            data=date(2026, 7, 28),
            materiale="Prodotto finito",
            note="Nota iniziale",
            completato_il=datetime(2026, 7, 28, 10, 30),
            completato_da_nome="operatore",
            creato_da_nome="operatore",
        )
        db.session.add_all([vettore, movimento])
        db.session.commit()
        movimento_id = movimento.id

        monkeypatch.setattr(
            logistica_routes,
            "_redirect_logistica",
            lambda: None,
        )
        monkeypatch.setattr(
            logistica_routes,
            "flash",
            lambda *_args: None,
        )
        with app.test_request_context(
            method="POST",
            data={"note": "Nota aggiornata"},
        ):
            logistica_routes.logistica_movimento_note.__wrapped__(
                movimento_id
            )

        db.session.expire_all()
        assert db.session.get(
            MovimentoLogistico,
            movimento_id,
        ).note == "Nota aggiornata"


def test_note_update_requires_carica_permission(app, monkeypatch):
    checked_permissions = []
    monkeypatch.setattr(
        policy_decorator,
        "resolve_operator_session",
        lambda: object(),
    )
    monkeypatch.setattr(
        policy_decorator,
        "active_policy",
        lambda: SimpleNamespace(
            can=lambda permission: checked_permissions.append(permission) or False
        ),
    )

    with app.test_request_context(method="POST"):
        with pytest.raises(Forbidden):
            logistica_routes.logistica_movimento_note(1)
