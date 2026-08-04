import re
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from flask import Flask
from sqlalchemy import inspect
from werkzeug.datastructures import MultiDict
from werkzeug.exceptions import Forbidden

from app_odp.logistica_models import (
    ClientePackingList,
    ImpostazioniPackingList,
    LOGISTICA_BIND_KEY,
    MovimentoLogistico,
    PackingList,
    RigaPackingList,
    VettoreTrasporto,
)
from app_odp.models import db
from app_odp.policy import decorator as policy_decorator
from app_odp.routes_modules import logistica as logistica_routes
from app_odp.routes_modules.logistica import (
    _company_header,
    _later_date,
    _packing_header,
    _packing_rows,
    _row_class,
)
from app_odp.services.packing_list_pdf_service import _number, build_packing_list_pdf


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


def test_logistica_creates_tables_and_persists_a_movement(app):
    with app.app_context():
        assert set(inspect(db.engines[LOGISTICA_BIND_KEY]).get_table_names()) == {
            "movimenti",
            "packing_clienti",
            "packing_impostazioni",
            "packing_list_righe",
            "packing_lists",
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


def test_packing_rows_require_all_four_editable_columns(app):
    with app.test_request_context(
        method="POST",
        data=MultiDict(
            [
                ("item_code", "ART-1"),
                ("item_description", "Descrizione libera"),
                ("item_serial_number", "SN-001"),
                ("item_quantity", "2,5"),
            ]
        ),
    ):
        assert _packing_rows() == [
            ("ART-1", "Descrizione libera", "SN-001", Decimal("2.5"))
        ]

    with app.test_request_context(
        method="POST",
        data=MultiDict(
            [
                ("item_code", "ART-1"),
                ("item_description", "Descrizione libera"),
                ("item_serial_number", ""),
                ("item_quantity", "2"),
            ]
        ),
    ):
        with pytest.raises(
            ValueError,
            match="Code, Description, Serial number e Quantity",
        ):
            _packing_rows()


def test_packing_header_normalizes_and_limits_lines(app):
    with app.test_request_context(
        method="POST",
        data={"company_header": " Azienda S.r.l. \n Via Roma 1 "},
    ):
        assert _packing_header() == "Azienda S.r.l.\nVia Roma 1"

    with app.test_request_context(
        method="POST",
        data={"company_header": "\n".join(str(index) for index in range(7))},
    ):
        with pytest.raises(ValueError, match="massimo 6 righe"):
            _packing_header()


def test_packing_header_is_saved_as_a_global_setting(app, monkeypatch):
    monkeypatch.setattr(logistica_routes, "_redirect_packing_list", lambda: None)
    monkeypatch.setattr(logistica_routes, "flash", lambda *_args: None)

    with app.test_request_context(
        method="POST",
        data={
            "company_header": (
                "Azienda personalizzata S.r.l.\n"
                "Via Prova 10\n10100 Torino\nITALY"
            )
        },
    ):
        logistica_routes.packing_list_header_update.__wrapped__()

    with app.app_context():
        settings = db.session.get(ImpostazioniPackingList, 1)
        assert settings.intestazione_pdf.startswith(
            "Azienda personalizzata S.r.l."
        )
        assert _company_header() == settings.intestazione_pdf


def test_packing_list_is_saved_and_can_be_printed(app, monkeypatch):
    form = MultiDict(
        [
            ("cliente_id", "new"),
            ("nuovo_cliente_nome", "Cliente S.p.A."),
            ("nuovo_cliente_indirizzo", "Via Roma 1"),
            ("nuovo_cliente_provincia", "CN"),
            ("nuovo_cliente_paese", "ITALY"),
            ("transport_document", "DDT-42"),
            ("invoice_number", "INV-7"),
            ("invoice_date", "2026-08-03"),
            ("total_pallets", "2"),
            ("total_net_weight", "125.5"),
            ("total_gross_weight", "140.75"),
            ("comments", "Maneggiare con cura"),
            ("delivery_nome", "Magazzino Cliente"),
            ("delivery_indirizzo", "Via Torino 10"),
            ("delivery_provincia", "TO"),
            ("delivery_paese", "ITALY"),
            ("item_code", "ART-1"),
            ("item_description", "Primo articolo"),
            ("item_serial_number", "SN-001"),
            ("item_quantity", "2.5"),
            ("delivery_terms", "DAP"),
            ("forwarder", "Trasporti Rossi"),
        ]
    )

    monkeypatch.setattr(
        logistica_routes,
        "active_user",
        lambda: SimpleNamespace(id=7, username="operatore"),
    )
    monkeypatch.setattr(logistica_routes, "_redirect_packing_list", lambda: None)
    monkeypatch.setattr(logistica_routes, "flash", lambda *_args: None)

    with app.test_request_context(method="POST", data=form):
        logistica_routes.packing_list_create.__wrapped__()

    with app.app_context():
        packing = PackingList.query.one()
        packing_id = packing.id
        assert packing.cliente.nome == "Cliente S.p.A."
        assert packing.delivery.nome == "Magazzino Cliente"
        assert packing.creato_da_nome == "operatore"
        assert RigaPackingList.query.one().numero_seriale == "SN-001"

    with app.test_request_context():
        response = logistica_routes.packing_list_pdf.__wrapped__(packing_id)
        response.direct_passthrough = False
        assert response.get_data().startswith(b"%PDF-")
        assert response.headers["Content-Disposition"].startswith("inline;")

    with app.app_context():
        assert set(inspect(db.engines[LOGISTICA_BIND_KEY]).get_table_names()) == {
            "movimenti",
            "packing_clienti",
            "packing_impostazioni",
            "packing_list_righe",
            "packing_lists",
            "vettori",
        }


def test_packing_customer_can_be_updated_and_deleted(app, monkeypatch):
    with app.app_context():
        cliente = ClientePackingList(
            nome="Cliente iniziale",
            indirizzo="Via Roma 1",
            provincia="CN",
            paese="ITALY",
        )
        db.session.add(cliente)
        db.session.commit()
        cliente_id = cliente.id

    monkeypatch.setattr(logistica_routes, "_redirect_packing_list", lambda: None)
    monkeypatch.setattr(logistica_routes, "flash", lambda *_args: None)

    with app.test_request_context(
        method="POST",
        data={
            "nome": "Cliente aggiornato",
            "indirizzo": "Via Torino 10",
            "provincia": "TO",
            "paese": "ITALY",
        },
    ):
        logistica_routes.packing_list_cliente_update.__wrapped__(cliente_id)

    with app.app_context():
        assert db.session.get(ClientePackingList, cliente_id).nome == (
            "Cliente aggiornato"
        )

    with app.test_request_context(method="POST"):
        logistica_routes.packing_list_cliente_delete.__wrapped__(cliente_id)

    with app.app_context():
        assert db.session.get(ClientePackingList, cliente_id) is None


def test_packing_list_pdf_contains_a_valid_multipage_document(app):
    with app.app_context():
        packing = SimpleNamespace(
            cliente=SimpleNamespace(
                nome="Cliente S.p.A.",
                indirizzo="Via Roma 1",
                provincia="CN",
                paese="ITALY",
            ),
            delivery=SimpleNamespace(
                nome="Magazzino Cliente",
                indirizzo="Via Torino 10",
                provincia="TO",
                paese="ITALY",
            ),
            transport_document="DDT-42",
            invoice_number="INV-7",
            invoice_date=date(2026, 8, 3),
            total_pallets=1,
            total_net_weight=Decimal("10"),
            total_gross_weight=Decimal("12"),
            comments=None,
            delivery_terms="EXW",
            forwarder="Trasporti Rossi",
            righe=[
                SimpleNamespace(
                    codice=f"ART-{index}",
                    descrizione=f"Descrizione compilata liberamente {index}",
                    numero_seriale=f"SN-{index:04d}",
                    quantita=Decimal(index),
                )
                for index in range(1, 71)
            ],
        )

        payload = build_packing_list_pdf(
            packing,
            company_header=(
                "Azienda personalizzata S.r.l.\n"
                "Via Prova 10\n10100 Torino\nITALY"
            ),
        ).getvalue()

        assert payload.startswith(b"%PDF-")
        assert len(payload) > 1_000
        assert len(re.findall(rb"/Type\s*/Page\b", payload)) >= 3


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("100"), "100"),
        (Decimal("10.500"), "10.5"),
        (Decimal("0.000"), "0"),
    ],
)
def test_packing_list_pdf_number_format_preserves_integer_zeroes(value, expected):
    assert _number(value) == expected
