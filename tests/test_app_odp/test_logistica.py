from datetime import date
from types import SimpleNamespace

import pytest
from flask import Flask
from sqlalchemy import inspect

from app_odp.logistica_models import (
    LOGISTICA_BIND_KEY,
    MovimentoLogistico,
    VettoreTrasporto,
)
from app_odp.models import db
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
