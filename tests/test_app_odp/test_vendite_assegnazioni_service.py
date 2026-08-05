from datetime import date
from types import SimpleNamespace

import pytest
from flask import Flask

from app_odp.models import InputOdp, db
from app_odp.services.vendite_assegnazioni_service import (
    VenditeAssegnazioniConflictError,
    VenditeAssegnazioniError,
    build_assignment_dashboard,
    create_customer_order,
    set_machine_assignment,
)
from app_odp.vendite_models import (
    VenditeOrdineCliente,
    VenditeOrdineClienteRiga,
)


ACTOR = SimpleNamespace(id=None, username="commerciale")


@pytest.fixture()
def app():
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)
    with app.app_context():
        db.create_all(bind_key=None)
    yield app
    with app.app_context():
        db.session.remove()
        db.drop_all(bind_key=None)


def _add_machine(
    *,
    document="DOC-1",
    row="1",
    model="MODELLO-1",
    variant="V1",
    serial="MAT-001",
):
    machine = InputOdp(
        IdDocumento=document,
        IdRiga=row,
        RifRegistraz="2026.1.10",
        NumProgrRiga=row,
        CodArt=model,
        VarianteArt=variant,
        DesArt=f"Macchina {model}",
        CodMatricola=serial,
        GestioneMatricola="si",
        StatoOrdineErp="Pianificata",
    )
    db.session.add(machine)
    db.session.flush()
    return machine


def _model_key(model="MODELLO-1", variant="V1"):
    return next(
        item["key"]
        for item in build_assignment_dashboard()["models"]
        if item["model_code"] == model and item["variant"] == variant
    )


def _payload(*, model_key, quantity=1, customer="Cliente S.p.A.", order="OC-10"):
    return {
        "customer_name": customer,
        "customer_order": order,
        "lines": [
            {
                "model_key": model_key,
                "quantity": quantity,
                "note": "Con accessorio speciale",
                "delivery_date": "2026-10-15",
            }
        ],
    }


def test_quantity_three_expands_rows_preserving_note_and_delivery_date(app):
    with app.app_context():
        _add_machine()

        customer_order = create_customer_order(
            _payload(model_key=_model_key(), quantity=3),
            ACTOR,
        )

        assert [row.posizione for row in customer_order.righe] == [1, 2, 3]
        assert {row.note for row in customer_order.righe} == {
            "Con accessorio speciale"
        }
        assert {row.data_consegna for row in customer_order.righe} == {
            date(2026, 10, 15)
        }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("quantity", 0),
        ("quantity", "1.5"),
        ("delivery_date", "domani"),
    ],
)
def test_invalid_customer_order_lines_are_rejected_atomically(app, field, value):
    with app.app_context():
        _add_machine()
        payload = _payload(model_key=_model_key())
        payload["lines"][0][field] = value

        with pytest.raises(VenditeAssegnazioniError):
            create_customer_order(payload, ACTOR)

        assert VenditeOrdineCliente.query.count() == 0
        assert VenditeOrdineClienteRiga.query.count() == 0


def test_duplicate_customer_and_order_number_is_rejected(app):
    with app.app_context():
        _add_machine()
        model_key = _model_key()
        create_customer_order(
            _payload(model_key=model_key, customer="Èlite S.p.A.", order="OC-10"),
            ACTOR,
        )

        with pytest.raises(VenditeAssegnazioniConflictError):
            create_customer_order(
                _payload(
                    model_key=model_key,
                    customer=" e\u0300LITE s.p.a. ",
                    order=" oc-10 ",
                ),
                ACTOR,
            )

        assert VenditeOrdineCliente.query.count() == 1


def test_compatible_assignment_is_exposed_by_dashboard(app):
    with app.app_context():
        machine = _add_machine()
        customer_order = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )
        customer_row = customer_order.righe[0]

        set_machine_assignment(
            customer_row.id,
            {
                "version": customer_row.versione,
                "id_documento": machine.IdDocumento,
                "id_riga": machine.IdRiga,
            },
            ACTOR,
        )
        dashboard = build_assignment_dashboard()

        assert customer_row.odp_matricola == "MAT-001"
        assert dashboard["summary"] == {
            "open_machines": 1,
            "total_demand": 1,
            "assigned_demand": 1,
            "unassigned_demand": 0,
        }
        assert dashboard["machines"][0]["assignment"] == {
            "row_id": customer_row.id,
            "customer_name": "Cliente S.p.A.",
            "customer_order": "OC-10",
        }
        assert dashboard["customer_orders"][0]["rows"][0]["assignment"][
            "present"
        ] is True
        assert dashboard["customer_orders"][0]["rows"][0]["assignment"][
            "open"
        ] is True
        assert dashboard["customer_orders"][0]["rows"][0]["version"] == (
            customer_row.versione
        )


def test_stale_assignment_version_is_rejected(app):
    with app.app_context():
        first_machine = _add_machine()
        second_machine = _add_machine(
            document="DOC-2",
            row="2",
            serial="MAT-002",
        )
        customer_order = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )
        customer_row = customer_order.righe[0]
        stale_version = customer_row.versione

        set_machine_assignment(
            customer_row.id,
            {
                "version": stale_version,
                "id_documento": first_machine.IdDocumento,
                "id_riga": first_machine.IdRiga,
            },
            ACTOR,
        )

        with pytest.raises(VenditeAssegnazioniConflictError, match="modificata"):
            set_machine_assignment(
                customer_row.id,
                {
                    "version": stale_version,
                    "id_documento": second_machine.IdDocumento,
                    "id_riga": second_machine.IdRiga,
                },
                ACTOR,
            )


def test_double_assignment_and_different_model_are_rejected(app):
    with app.app_context():
        first_machine = _add_machine()
        different_model = _add_machine(
            document="DOC-2",
            row="2",
            model="MODELLO-2",
            variant="V2",
            serial="MAT-002",
        )
        customer_order = create_customer_order(
            _payload(model_key=_model_key(), quantity=2),
            ACTOR,
        )
        first_row, second_row = customer_order.righe
        set_machine_assignment(
            first_row.id,
            {
                "version": first_row.versione,
                "id_documento": first_machine.IdDocumento,
                "id_riga": first_machine.IdRiga,
            },
            ACTOR,
        )

        with pytest.raises(
            VenditeAssegnazioniConflictError,
            match="assegnato a un altro",
        ):
            set_machine_assignment(
                second_row.id,
                {
                    "version": second_row.versione,
                    "id_documento": first_machine.IdDocumento,
                    "id_riga": first_machine.IdRiga,
                },
                ACTOR,
            )
        with pytest.raises(VenditeAssegnazioniConflictError, match="non corrisponde"):
            set_machine_assignment(
                second_row.id,
                {
                    "version": second_row.versione,
                    "id_documento": different_model.IdDocumento,
                    "id_riga": different_model.IdRiga,
                },
                ACTOR,
            )


def test_assignment_can_be_cleared(app):
    with app.app_context():
        machine = _add_machine()
        customer_order = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )
        customer_row = customer_order.righe[0]
        set_machine_assignment(
            customer_row.id,
            {
                "version": customer_row.versione,
                "id_documento": machine.IdDocumento,
                "id_riga": machine.IdRiga,
            },
            ACTOR,
        )

        set_machine_assignment(
            customer_row.id,
            {"version": customer_row.versione},
            ACTOR,
        )
        dashboard = build_assignment_dashboard()

        assert customer_row.odp_id_documento is None
        assert customer_row.odp_id_riga is None
        assert customer_row.assegnata_il is None
        assert dashboard["summary"]["assigned_demand"] == 0
        assert dashboard["machines"][0]["assignment"] is None
        assert dashboard["customer_orders"][0]["rows"][0]["assignment"] is None
