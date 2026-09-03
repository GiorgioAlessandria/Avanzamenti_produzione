from datetime import date, timedelta
from types import SimpleNamespace

import pytest
from flask import Flask

from app_odp.models import AcqArticoliLookup, InputOdp, InputOdpLog, db
from app_odp.services.ordini_runtime_service import _ensure_stato_attivo
from app_odp.services.order_helpers import _now_rome_dt
from app_odp.services.vendite_assegnazioni_service import (
    VenditeAssegnazioniConflictError,
    VenditeAssegnazioniError,
    auto_assign_activated_machine,
    build_assignment_dashboard,
    confirm_customer_order_shipment,
    confirm_customer_order_read,
    confirm_customer_row_shipment,
    create_customer_order,
    delete_customer_order,
    register_closed_machine_stock,
    set_machine_assignment,
    ship_stock_machine,
    update_customer_order_details,
    update_customer_row,
    update_customer_row_dates,
    update_customer_row_notes,
    update_packaging_notes,
    update_machine_production_note,
    validate_closed_machine_stock,
)
from app_odp.vendite_models import (
    VenditeNotaProduzioneMacchina,
    VenditeMacchinaStock,
    VenditeOrdineCliente,
    VenditeOrdineClienteRiga,
    VenditeSpedizioneConfermata,
)


ACTOR = SimpleNamespace(id=None, username="commerciale")


def _machine_note_payload(machine, note, version=0):
    return {
        "id_documento": machine.IdDocumento,
        "id_riga": machine.IdRiga,
        "serial_number": machine.CodMatricola,
        "production_note": note,
        "version": version,
    }


def _assign_test_machine(row, machine):
    return set_machine_assignment(
        row.id,
        {"version": row.versione, "id_documento": machine.IdDocumento,
         "id_riga": machine.IdRiga},
        ACTOR,
    )


@pytest.fixture()
def app():
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_BINDS={"acq": "sqlite://", "log": "sqlite://"},
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)
    with app.app_context():
        db.create_all(bind_key=None)
        db.create_all(bind_key="acq")
        # Questi test usano solo InputOdpLog, non gli altri log applicativi.
        InputOdpLog.__table__.create(db.engines["log"])
    yield app
    with app.app_context():
        db.session.remove()
        InputOdpLog.__table__.drop(db.engines["log"])
        db.drop_all(bind_key="acq")
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


def _add_known_model(*, model="MODELLO-1", variant="V1"):
    item = AcqArticoliLookup(
        CodArt=model,
        VarianteArt=variant,
        IndiceModifica="A",
        DesArt=f"Macchina {model}",
        GestioneMatricola="si",
    )
    db.session.add(item)
    db.session.flush()
    return item


def _closed_machine(
    *,
    document="DOC-STOCK-1",
    row="1",
    model="MODELLO-1",
    variant="V1",
    serial="123456",
):
    return SimpleNamespace(
        IdDocumento=document,
        IdRiga=row,
        RifRegistraz="2026.1.99",
        NumProgrRiga=row,
        CodArt=model,
        VarianteArt=variant,
        DesArt=f"Macchina {model}",
        CodMatricola=serial,
    )


def _payload(
    *,
    model_key,
    quantity=1,
    customer="Cliente S.p.A.",
    order="OC-10",
    delivery_date="2026-10-15",
    internal_reference="ITALIA",
):
    return {
        "customer_name": customer,
        "customer_order": order,
        "internal_reference": internal_reference,
        "lines": [
            {
                "model_key": model_key,
                "quantity": quantity,
                "sales_note": "Con accessorio speciale",
                "delivery_date": delivery_date,
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
        assert {row.data_disponibile for row in customer_order.righe} == {None}


def test_customer_order_can_be_created_before_machine_order_exists(app):
    with app.app_context():
        _add_known_model()

        customer_order = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )
        dashboard = build_assignment_dashboard()

        assert len(customer_order.righe) == 1
        assert customer_order.righe[0].odp_id_documento is None
        assert dashboard["summary"]["open_machines"] == 0
        assert dashboard["summary"]["unassigned_demand"] == 1


def test_internal_reference_sets_editable_packaging_note_and_row_delivery(app):
    with app.app_context():
        _add_known_model()

        customer_order = create_customer_order(
            _payload(
                model_key=_model_key(),
                internal_reference="EXTRACEE",
                delivery_date="2026-10-10",
            ),
            ACTOR,
        )
        dashboard_order = build_assignment_dashboard()["customer_orders"][0]

        assert customer_order.riferimento_interno == "EXTRACEE"
        assert customer_order.data_spedizione == date(2026, 10, 10)
        assert customer_order.righe[0].note_spedizione == (
            "Inserire sacchetto anti-umidità"
        )
        assert dashboard_order["internal_reference"] == "EXTRACEE"
        assert dashboard_order["rows"][0]["delivery_date"] == "2026-10-10"
        assert dashboard_order["rows"][0]["available_date"] == ""


def test_reference_change_preserves_custom_packaging_notes(app):
    with app.app_context():
        _add_known_model()
        customer_order = create_customer_order(
            _payload(
                model_key=_model_key(),
                quantity=2,
                internal_reference="EXTRACEE",
            ),
            ACTOR,
        )
        custom_row, default_row = customer_order.righe
        update_customer_row_notes(
            custom_row.id,
            {
                "version": custom_row.versione,
                "shipping_note": "Imballo concordato con il cliente",
            },
            can_edit_sales=True,
            can_edit_production=False,
        )

        update_customer_order_details(
            customer_order.id,
            {"internal_reference": "ESTERO"},
        )

        assert customer_order.riferimento_interno == "ESTERO"
        assert customer_order.data_spedizione == date(2026, 10, 15)
        assert {row.data_consegna for row in customer_order.righe} == {
            date(2026, 10, 15)
        }
        assert custom_row.note_spedizione == "Imballo concordato con il cliente"
        assert default_row.note_spedizione is None


def test_packaging_note_configuration_updates_only_default_values(app):
    with app.app_context():
        _add_known_model()
        customer_order = create_customer_order(
            _payload(
                model_key=_model_key(),
                internal_reference="EXTRACEE",
            ),
            ACTOR,
        )

        update_packaging_notes(
            {
                "notes": {
                    "ITALIA": "",
                    "ESTERO": "",
                    "EXTRACEE": "Inserire due sacchetti anti-umidità",
                }
            },
            ACTOR,
        )

        assert customer_order.righe[0].note_spedizione == (
            "Inserire due sacchetti anti-umidità"
        )
        packaging = {
            item["reference"]: item["note"]
            for item in build_assignment_dashboard()["packaging_notes"]
        }
        assert packaging["EXTRACEE"] == "Inserire due sacchetti anti-umidità"


def test_invalid_internal_reference_is_rejected(app):
    with app.app_context():
        _add_known_model()
        payload = _payload(model_key=_model_key())
        payload["internal_reference"] = "EUROPA"

        with pytest.raises(VenditeAssegnazioniError, match="riferimento interno"):
            create_customer_order(payload, ACTOR)

        assert VenditeOrdineCliente.query.count() == 0


def test_row_dates_follow_separate_permissions_and_can_clear_available(app):
    with app.app_context():
        _add_known_model()
        row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]

        update_customer_row_dates(
            row.id,
            {
                "version": row.versione,
                "available_date": "2026-10-12",
                "delivery_date": "2026-10-20",
            },
            can_edit_delivery=False,
            can_edit_available=True,
        )
        assert row.data_disponibile == date(2026, 10, 12)
        assert row.data_consegna == date(2026, 10, 15)

        update_customer_row_dates(
            row.id,
            {
                "version": row.versione,
                "available_date": "",
                "delivery_date": "2026-10-20",
            },
            can_edit_delivery=True,
            can_edit_available=False,
        )
        dashboard_row = build_assignment_dashboard()["customer_orders"][0][
            "rows"
        ][0]
        assert row.data_disponibile == date(2026, 10, 12)
        assert row.data_consegna == date(2026, 10, 20)
        assert dashboard_row["available_date"] == "2026-10-12"
        assert dashboard_row["delivery_date"] == "2026-10-20"

        update_customer_row_dates(
            row.id,
            {"version": row.versione, "available_date": ""},
            can_edit_delivery=False,
            can_edit_available=True,
        )
        assert row.data_disponibile is None


def test_single_row_save_updates_assignment_dates_and_notes(app):
    with app.app_context():
        _add_known_model()
        row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]
        machine = _add_machine()

        update_customer_row(
            row.id,
            {
                "version": row.versione,
                "available_date": "2026-10-12",
                "production_note": "Pronta al collaudo",
                "assignment_changed": True,
                "id_documento": machine.IdDocumento,
                "id_riga": machine.IdRiga,
            },
            ACTOR,
            can_edit_sales=False,
            can_edit_production=True,
            can_assign=True,
        )

        assert row.data_disponibile == date(2026, 10, 12)
        assert row.note_produzione == "Pronta al collaudo"
        assert row.odp_matricola == "MAT-001"
        assert row.assegnazione_automatica is False


@pytest.mark.parametrize("phase", ["1", "2"])
def test_phase_one_or_two_activation_auto_assigns_nearest_exact_model(app, phase):
    with app.app_context():
        _add_known_model()
        _add_known_model(model="MODELLO-2", variant="V2")
        today = _now_rome_dt().date()
        farther = create_customer_order(
            _payload(
                model_key=_model_key(),
                customer="Cliente lontano",
                order="OC-LONTANO",
                delivery_date=(today + timedelta(days=12)).isoformat(),
            ),
            ACTOR,
        ).righe[0]
        nearest = create_customer_order(
            _payload(
                model_key=_model_key(),
                customer="Cliente vicino",
                order="OC-VICINO",
                delivery_date=(today + timedelta(days=2)).isoformat(),
            ),
            ACTOR,
        ).righe[0]
        other_model = create_customer_order(
            _payload(
                model_key=_model_key("MODELLO-2", "V2"),
                customer="Altro modello",
                order="OC-ALTRO",
                delivery_date=today.isoformat(),
            ),
            ACTOR,
        ).righe[0]
        machine = _add_machine()

        _ensure_stato_attivo(
            ordine=machine,
            stato=None,
            username="operatore",
            when_dt=_now_rome_dt(),
            fase_corrente=phase,
        )
        db.session.flush()
        dashboard = build_assignment_dashboard()

        assert farther.odp_id_documento is None
        assert other_model.odp_id_documento is None
        assert nearest.odp_matricola == "MAT-001"
        assert nearest.assegnazione_automatica is True
        nearest_payload = next(
            row
            for order in dashboard["customer_orders"]
            for row in order["rows"]
            if row["id"] == nearest.id
        )
        assert nearest_payload["assignment"]["automatic"] is True


def test_phase_three_activation_does_not_auto_assign(app):
    with app.app_context():
        _add_known_model()
        customer_row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]
        machine = _add_machine()

        _ensure_stato_attivo(
            ordine=machine,
            stato=None,
            username="operatore",
            when_dt=_now_rome_dt(),
            fase_corrente="3",
        )
        db.session.flush()

        assert customer_row.odp_id_documento is None
        assert customer_row.assegnazione_automatica is False


def test_customer_order_read_confirmation_is_recorded(app):
    with app.app_context():
        _add_known_model()
        customer_order = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )

        assert build_assignment_dashboard()["customer_orders"][0][
            "read_confirmed"
        ] is False

        confirm_customer_order_read(customer_order.id, ACTOR)
        dashboard = build_assignment_dashboard()

        assert customer_order.confermato_il
        assert customer_order.confermato_da_nome == "commerciale"
        assert dashboard["customer_orders"][0]["read_confirmed"] is True


def test_delete_customer_order_releases_assigned_machines(app):
    with app.app_context():
        machine = _add_machine()
        customer_order = create_customer_order(
            _payload(model_key=_model_key(), quantity=2),
            ACTOR,
        )
        _assign_test_machine(customer_order.righe[0], machine)
        assert customer_order.righe[0].odp_matricola == "MAT-001"
        assert build_assignment_dashboard()["machines"] == []

        delete_customer_order(customer_order.id)
        dashboard = build_assignment_dashboard()

        assert VenditeOrdineCliente.query.count() == 0
        assert VenditeOrdineClienteRiga.query.count() == 0
        assert dashboard["customer_orders"] == []
        assert dashboard["summary"]["total_demand"] == 0
        assert dashboard["summary"]["assigned_demand"] == 0
        assert dashboard["machines"][0]["serial_number"] == "MAT-001"
        assert dashboard["assignment_machines"][0]["assigned"] is False


def test_optional_machine_requires_one_unit_and_becomes_unavailable(app):
    with app.app_context():
        machine = _add_machine()
        payload = _payload(model_key=_model_key(), quantity=1)
        payload["lines"][0].update(
            {
                "id_documento": machine.IdDocumento,
                "id_riga": machine.IdRiga,
            }
        )

        customer_order = create_customer_order(payload, ACTOR)
        dashboard = build_assignment_dashboard()

        assert len(customer_order.righe) == 1
        assert customer_order.righe[0].odp_matricola == "MAT-001"
        assert customer_order.righe[0].assegnazione_automatica is False
        assert dashboard["machines"] == []
        assert dashboard["assignment_machines"][0]["assigned"] is True
        assert dashboard["assignment_machines"][0]["assigned_row_id"] == (
            customer_order.righe[0].id
        )
        assert dashboard["summary"] == {
            "open_machines": 0,
            "total_demand": 1,
            "assigned_demand": 1,
            "unassigned_demand": 0,
        }


def test_optional_machine_rejects_quantity_greater_than_one(app):
    with app.app_context():
        machine = _add_machine()
        payload = _payload(model_key=_model_key(), quantity=2)
        payload["lines"][0].update(
            {
                "id_documento": machine.IdDocumento,
                "id_riga": machine.IdRiga,
            }
        )

        with pytest.raises(VenditeAssegnazioniError, match="quantità deve essere 1"):
            create_customer_order(payload, ACTOR)

        assert VenditeOrdineCliente.query.count() == 0


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
        target = payload["lines"][0]
        target[field] = value

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


def test_creation_leaves_available_machine_unassigned(app):
    with app.app_context():
        _add_machine()
        customer_order = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )
        customer_row = customer_order.righe[0]
        dashboard = build_assignment_dashboard()

        assert customer_row.odp_id_documento is None
        assert customer_row.odp_id_riga is None
        assert customer_row.odp_matricola is None
        assert customer_row.assegnazione_automatica is False
        assert dashboard["summary"] == {
            "open_machines": 1,
            "total_demand": 1,
            "assigned_demand": 0,
            "unassigned_demand": 1,
        }
        assert dashboard["machines"][0]["serial_number"] == "MAT-001"
        assert dashboard["customer_orders"][0]["rows"][0]["assignment"] is None


def test_manual_machine_change_removes_automatic_marker(app):
    with app.app_context():
        machine = _add_machine()
        second_machine = _add_machine(
            document="DOC-2",
            row="2",
            serial="MAT-002",
        )
        customer_row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]
        auto_assign_activated_machine(machine, phase="1")
        assert customer_row.odp_matricola == "MAT-001"
        assert customer_row.assegnazione_automatica is True

        set_machine_assignment(
            customer_row.id,
            {
                "version": customer_row.versione,
                "id_documento": second_machine.IdDocumento,
                "id_riga": second_machine.IdRiga,
            },
            ACTOR,
        )
        dashboard = build_assignment_dashboard()

        assert customer_row.odp_matricola == "MAT-002"
        assert customer_row.assegnazione_automatica is False
        assert dashboard["customer_orders"][0]["rows"][0]["assignment"][
            "automatic"
        ] is False
        assert dashboard["machines"][0]["serial_number"] == "MAT-001"


def test_manual_save_of_same_machine_removes_automatic_marker(app):
    with app.app_context():
        machine = _add_machine()
        customer_row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]

        auto_assign_activated_machine(machine, phase="1")

        set_machine_assignment(
            customer_row.id,
            {
                "version": customer_row.versione,
                "id_documento": machine.IdDocumento,
                "id_riga": machine.IdRiga,
            },
            ACTOR,
        )

        assert customer_row.assegnazione_automatica is False


def test_phase_two_closed_marks_customer_machine_and_order_completed(app):
    with app.app_context():
        machine = _add_machine()
        customer_row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]

        _assign_test_machine(customer_row, machine)

        machine.FaseAttiva = "2"
        machine.StatoOrdine = "Chiusa"
        db.session.flush()
        dashboard = build_assignment_dashboard()
        customer = dashboard["customer_orders"][0]
        row = customer["rows"][0]

        assert customer_row.odp_matricola == "MAT-001"
        assert row["completed"] is True
        assert row["assignment"]["completed"] is True
        assert row["assignment"]["phase"] == "2"
        assert row["assignment"]["state"] == "Chiusa"
        assert customer["completed_rows"] == 1
        assert customer["completed"] is True


def test_phase_two_closed_remains_completed_after_machine_sync_removal(app):
    with app.app_context():
        machine = _add_machine()
        customer = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )
        _assign_test_machine(customer.righe[0], machine)
        db.session.add(
            InputOdpLog(
                OperationGroupId="CHIUSURA-1",
                IdDocumento=machine.IdDocumento,
                IdRiga=machine.IdRiga,
                FaseConsuntivata="2",
                StatoOrdinePost="Chiusa",
                ClosedAt=_now_rome_dt().isoformat(timespec="seconds"),
            )
        )
        db.session.delete(machine)
        db.session.flush()

        row = build_assignment_dashboard()["customer_orders"][0]["rows"][0]

        assert row["completed"] is True
        assert row["assignment"]["present"] is False
        assert row["assignment"]["phase"] == "2"
        assert row["assignment"]["state"] == "Chiusa"


def test_sales_and_production_notes_follow_separate_permissions(app):
    with app.app_context():
        _add_known_model()
        customer_row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]

        update_customer_row_notes(
            customer_row.id,
            {
                "version": customer_row.versione,
                "sales_note": "Nota commerciale aggiornata",
                "shipping_note": "Consegna al magazzino nord",
                "production_note": "Non autorizzata",
            },
            can_edit_sales=True,
            can_edit_production=False,
        )
        assert customer_row.note == "Nota commerciale aggiornata"
        assert customer_row.note_spedizione == "Consegna al magazzino nord"
        assert customer_row.note_produzione is None

        update_customer_row_notes(
            customer_row.id,
            {
                "version": customer_row.versione,
                "sales_note": "Non autorizzata",
                "shipping_note": "Non autorizzata",
                "production_note": "Collaudo completato",
            },
            can_edit_sales=False,
            can_edit_production=True,
        )
        row = build_assignment_dashboard()["customer_orders"][0]["rows"][0]

        assert row["sales_note"] == "Nota commerciale aggiornata"
        assert row["shipping_note"] == "Consegna al magazzino nord"
        assert row["production_note"] == "Collaudo completato"


def test_shipment_confirmation_saves_timestamped_full_snapshot(app):
    with app.app_context():
        machine = _add_machine()
        customer_row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]
        _assign_test_machine(customer_row, machine)
        update_customer_row_notes(
            customer_row.id,
            {
                "version": customer_row.versione,
                "production_note": "Macchina collaudata",
            },
            can_edit_sales=False,
            can_edit_production=True,
        )
        update_customer_row_dates(
            customer_row.id,
            {
                "version": customer_row.versione,
                "available_date": "2026-10-12",
            },
            can_edit_delivery=False,
            can_edit_available=True,
        )
        machine.FaseAttiva = "2"
        machine.StatoOrdine = "Chiusa"
        db.session.flush()

        shipment = confirm_customer_row_shipment(
            customer_row.id,
            {
                "version": customer_row.versione,
                "sales_note": "Cliente avvisato",
                "shipping_note": "Corriere espresso",
                "delivery_date": "2026-10-18",
            },
            ACTOR,
        )
        dashboard_row = build_assignment_dashboard()["customer_orders"][0][
            "rows"
        ][0]

        assert VenditeSpedizioneConfermata.query.count() == 1
        assert shipment.cliente_nome == "Cliente S.p.A."
        assert shipment.numero_ordine == "OC-10"
        assert shipment.riferimento_interno == "ITALIA"
        assert shipment.data_spedizione == date(2026, 10, 18)
        assert shipment.data_disponibile == date(2026, 10, 12)
        assert shipment.data_consegna == date(2026, 10, 18)
        assert shipment.modello_codice == "MODELLO-1"
        assert shipment.odp_matricola == "MAT-001"
        assert shipment.note_vendita == "Cliente avvisato"
        assert shipment.note_produzione == "Macchina collaudata"
        assert shipment.note_spedizione == "Corriere espresso"
        assert shipment.confermata_il
        assert shipment.confermata_da_nome == "commerciale"
        assert dashboard_row["shipment"]["confirmed_at"] == shipment.confermata_il

        customer_row.note = "Modifica successiva"
        db.session.flush()
        assert shipment.note_vendita == "Cliente avvisato"


def test_shipment_confirmation_requires_phase_two_closed(app):
    with app.app_context():
        machine = _add_machine()
        customer_row = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        ).righe[0]
        _assign_test_machine(customer_row, machine)

        with pytest.raises(VenditeAssegnazioniConflictError, match="Fase 2 chiusa"):
            confirm_customer_row_shipment(
                customer_row.id,
                {
                    "version": customer_row.versione,
                    "sales_note": "Pronta",
                    "shipping_note": "",
                },
                ACTOR,
            )

        assert VenditeSpedizioneConfermata.query.count() == 0


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


def test_assignment_can_be_moved_but_different_model_is_rejected(app):
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
        assert first_row.assegnazione_automatica is False
        set_machine_assignment(
            first_row.id,
            {
                "version": first_row.versione,
                "id_documento": first_machine.IdDocumento,
                "id_riga": first_machine.IdRiga,
            },
            ACTOR,
        )

        set_machine_assignment(
            second_row.id,
            {
                "version": second_row.versione,
                "id_documento": first_machine.IdDocumento,
                "id_riga": first_machine.IdRiga,
            },
            ACTOR,
        )

        assert first_row.odp_id_documento is None
        assert second_row.odp_matricola == "MAT-001"
        assert second_row.assegnazione_automatica is False
        assignment_machine = build_assignment_dashboard()[
            "assignment_machines"
        ][0]
        assert assignment_machine["assigned_row_id"] == second_row.id
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
        assert customer_row.assegnazione_automatica is False
        assert dashboard["summary"]["assigned_demand"] == 0
        assert dashboard["machines"][0]["serial_number"] == "MAT-001"
        assert "assignment" not in dashboard["machines"][0]
        assert dashboard["customer_orders"][0]["rows"][0]["assignment"] is None


def test_closed_machine_is_registered_once_and_exposed_as_stock(app):
    with app.app_context():
        machine = _closed_machine()

        first = register_closed_machine_stock(
            machine,
            closed_at="2026-08-06T10:00:00+02:00",
            closed_by="produzione",
        )
        second = register_closed_machine_stock(machine, closed_by="produzione")
        dashboard = build_assignment_dashboard()

        assert first.id == second.id
        assert VenditeMacchinaStock.query.count() == 1
        assert first.odp_id_documento == "DOC-STOCK-1"
        assert first.matricola == "123456"
        assert first.inserita_da_nome == "produzione"
        assert dashboard["summary"]["open_machines"] == 1
        assert dashboard["machines"][0]["order"] == "STOCK"
        assert dashboard["machines"][0]["serial_number"] == "123456"
        assert dashboard["machines"][0]["phase"] == "2"
        assert dashboard["machines"][0]["state"] == "Chiusa"


def test_closed_assigned_machine_stays_hidden_until_unassigned(app):
    with app.app_context():
        machine = _add_machine(serial="123456")
        customer = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )
        customer_row = customer.righe[0]
        _assign_test_machine(customer_row, machine)
        assert customer_row.odp_rif_registraz == machine.RifRegistraz

        stock = register_closed_machine_stock(machine, closed_by="produzione")
        db.session.delete(machine)
        db.session.flush()
        assigned_dashboard = build_assignment_dashboard()

        assert stock is not None
        assert VenditeMacchinaStock.query.count() == 1
        assert assigned_dashboard["machines"] == []
        assert (
            assigned_dashboard["customer_orders"][0]["rows"][0]
            ["assignment"]["order"]
            != "STOCK"
        )

        set_machine_assignment(
            customer_row.id,
            {"version": customer_row.versione},
            ACTOR,
        )
        available_dashboard = build_assignment_dashboard()

        assert available_dashboard["machines"][0]["order"] == "STOCK"
        assert available_dashboard["machines"][0]["serial_number"] == "123456"


def test_unassigned_closure_requires_six_digit_serial(app):
    with app.app_context():
        machine = _closed_machine(serial="MAT-01")

        with pytest.raises(VenditeAssegnazioniError, match="esattamente 6 cifre"):
            validate_closed_machine_stock(machine)

        assert VenditeMacchinaStock.query.count() == 0


def test_assigned_closure_still_requires_six_digit_serial(app):
    with app.app_context():
        machine = _add_machine(serial="MAT-01")
        customer = create_customer_order(_payload(model_key=_model_key()), ACTOR)
        _assign_test_machine(customer.righe[0], machine)

        with pytest.raises(VenditeAssegnazioniError, match="esattamente 6 cifre"):
            validate_closed_machine_stock(machine)

        assert VenditeMacchinaStock.query.count() == 0


def test_stock_identity_collisions_are_rejected(app):
    with app.app_context():
        register_closed_machine_stock(_closed_machine(), closed_by="produzione")

        with pytest.raises(VenditeAssegnazioniConflictError, match="altro ordine"):
            register_closed_machine_stock(
                _closed_machine(document="DOC-STOCK-2"),
                closed_by="produzione",
            )
        with pytest.raises(VenditeAssegnazioniConflictError, match="matricola diversa"):
            register_closed_machine_stock(
                _closed_machine(serial="654321"),
                closed_by="produzione",
            )

        assert VenditeMacchinaStock.query.count() == 1


def test_stock_shipment_is_blocked_on_duplicate_production_serial(app):
    with app.app_context():
        register_closed_machine_stock(_closed_machine(), closed_by="produzione")
        duplicate = _add_machine(
            document="DOC-DUPLICATE",
            row="9",
            serial="123456",
        )

        dashboard = build_assignment_dashboard()
        assert len(dashboard["machines"]) == 1
        assert dashboard["machines"][0]["order"] == "STOCK"
        with pytest.raises(VenditeAssegnazioniConflictError, match="produzione"):
            ship_stock_machine("DOC-STOCK-1", "1")

        db.session.delete(duplicate)
        db.session.flush()
        ship_stock_machine("DOC-STOCK-1", "1")
        assert VenditeMacchinaStock.query.count() == 0


def test_unassigned_stock_machine_can_be_shipped_directly(app):
    with app.app_context():
        register_closed_machine_stock(_closed_machine(), closed_by="produzione")

        shipped = ship_stock_machine("DOC-STOCK-1", "1")

        assert shipped.matricola == "123456"
        assert VenditeMacchinaStock.query.count() == 0
        assert build_assignment_dashboard()["machines"] == []


def test_stock_machine_is_manually_assigned_and_removed_on_shipment(app):
    with app.app_context():
        register_closed_machine_stock(_closed_machine(), closed_by="produzione")
        customer = create_customer_order(
            _payload(model_key=_model_key()),
            ACTOR,
        )
        customer_row = customer.righe[0]
        assert customer_row.odp_matricola is None
        _assign_test_machine(customer_row, _closed_machine())
        assigned_dashboard = build_assignment_dashboard()

        assert customer_row.assegnazione_automatica is False
        assert customer_row.odp_rif_registraz == "STOCK"
        assert assigned_dashboard["machines"] == []
        assert (
            assigned_dashboard["customer_orders"][0]["rows"][0]
            ["assignment"]["order"]
            == "STOCK"
        )

        shipment = confirm_customer_row_shipment(
            customer_row.id,
            {
                "version": customer_row.versione,
                "sales_note": "Vendita aggiornata",
                "shipping_note": "Imballo aggiornato",
                "delivery_date": "2026-10-15",
            },
            ACTOR,
        )

        assert shipment.odp_matricola == "123456"
        assert VenditeMacchinaStock.query.count() == 0
        assert VenditeSpedizioneConfermata.query.count() == 1


def test_whole_order_shipment_removes_all_stock_machines(app):
    with app.app_context():
        register_closed_machine_stock(
            _closed_machine(document="DOC-STOCK-1", serial="123456"),
            closed_by="produzione",
        )
        register_closed_machine_stock(
            _closed_machine(document="DOC-STOCK-2", serial="654321"),
            closed_by="produzione",
        )
        customer = create_customer_order(
            _payload(model_key=_model_key(), quantity=2),
            ACTOR,
        )
        delivery_dates = ["2026-12-20", "2026-09-01"]
        for index, row in enumerate(customer.righe, start=1):
            _assign_test_machine(row, _closed_machine(document=f"DOC-STOCK-{index}"))
        rows_payload = [
            {
                "id": row.id,
                "version": row.versione,
                "sales_note": row.note or "",
                "shipping_note": row.note_spedizione or "",
                "delivery_date": delivery_dates[index],
            }
            for index, row in enumerate(customer.righe)
        ]

        confirm_customer_order_shipment(
            customer.id,
            {"rows": rows_payload},
            ACTOR,
        )
        dashboard_order = build_assignment_dashboard()["customer_orders"][0]

        assert VenditeMacchinaStock.query.count() == 0
        assert VenditeSpedizioneConfermata.query.count() == 2
        assert {
            shipment.data_spedizione
            for shipment in VenditeSpedizioneConfermata.query.all()
        } == {date(2026, 9, 1)}
        assert dashboard_order["shipment_ready"] is False
        assert all(row["shipment"] for row in dashboard_order["rows"])


def test_machine_production_note_follows_assignment_and_updates_both_views(app):
    from app_odp.services.vendite_service import build_vendite_payload

    with app.app_context():
        machine = _add_machine()
        note = update_machine_production_note(
            _machine_note_payload(machine, "Cablaggio speciale\\nCollaudo richiesto")
        )
        assert note.versione == 1
        assert build_vendite_payload()["machines"][0]["production_note"] == note.note
        customer = create_customer_order(_payload(model_key=_model_key()), ACTOR)
        row = customer.righe[0]
        assert row.odp_matricola is None
        update_customer_row(
            row.id,
            {"version": row.versione, "assignment_changed": True,
             "id_documento": machine.IdDocumento, "id_riga": machine.IdRiga,
             "production_note": "", "sales_note": row.note},
            ACTOR, can_edit_sales=True, can_edit_production=True, can_assign=True,
        )
        assert row.note_produzione == note.note
        assert build_assignment_dashboard()["customer_orders"][0]["rows"][0]["production_note"] == note.note
        old_row_version = row.versione
        update_machine_production_note(_machine_note_payload(machine, "Collaudo concluso", note.versione))
        assert row.note_produzione == "Collaudo concluso"
        assert row.versione > old_row_version
        update_customer_row_notes(
            row.id, {"version": row.versione, "production_note": "Aggiornata da ordini"},
            can_edit_sales=False, can_edit_production=True,
        )
        assert build_vendite_payload()["machines"][0]["production_note"] == "Aggiornata da ordini"
        set_machine_assignment(row.id, {"version": row.versione}, ACTOR)
        assert row.note_produzione is None
        assert build_vendite_payload()["machines"][0]["production_note"] == "Aggiornata da ordini"
        other = create_customer_order(_payload(model_key=_model_key(), order="OC-20"), ACTOR).righe[0]
        set_machine_assignment(
            other.id, {"version": other.versione, "id_documento": machine.IdDocumento,
                       "id_riga": machine.IdRiga}, ACTOR,
        )
        assert other.note_produzione == "Aggiornata da ordini"


def test_machine_production_note_rejects_stale_version_and_wrong_serial(app):
    with app.app_context():
        machine = _add_machine()
        update_machine_production_note(_machine_note_payload(machine, "Prima versione"))
        with pytest.raises(VenditeAssegnazioniConflictError):
            update_machine_production_note(_machine_note_payload(machine, "Obsoleta", 0))
        payload = _machine_note_payload(machine, "Errata", 1)
        payload["serial_number"] = "ALTRA-MATRICOLA"
        with pytest.raises(VenditeAssegnazioniConflictError):
            update_machine_production_note(payload)
        assert db.session.get(VenditeNotaProduzioneMacchina, "mat-001").note == "Prima versione"


@pytest.mark.parametrize("changes", [
    {"production_note": "x" * 1001}, {"production_note": None},
    {"version": True}, {"version": -1}, {"version": "1"},
    {"id_documento": ""}, {"serial_number": ""},
])
def test_machine_production_note_validates_input(app, changes):
    with app.app_context():
        machine = _add_machine()
        payload = {**_machine_note_payload(machine, "Valida"), **changes}
        with pytest.raises(VenditeAssegnazioniError):
            update_machine_production_note(payload)
        assert VenditeNotaProduzioneMacchina.query.count() == 0


def test_machine_production_note_keeps_explicit_empty_and_legacy_notes(app):
    from app_odp.services.vendite_service import build_vendite_payload

    with app.app_context():
        machine = _add_machine()
        row = create_customer_order(_payload(model_key=_model_key()), ACTOR).righe[0]
        set_machine_assignment(
            row.id, {"version": row.versione, "id_documento": machine.IdDocumento,
                     "id_riga": machine.IdRiga}, ACTOR,
        )
        # Simula un'assegnazione già presente prima dell'introduzione dell'archivio note.
        db.session.delete(db.session.get(VenditeNotaProduzioneMacchina, "mat-001"))
        row.note_produzione = "Nota precedente"
        db.session.flush()
        assert build_vendite_payload()["machines"][0]["production_note"] == "Nota precedente"
        update_machine_production_note(_machine_note_payload(machine, ""))
        assert row.note_produzione is None
        assert build_vendite_payload()["machines"][0]["production_note"] == ""


def test_machine_production_note_switch_does_not_copy_old_machine_note(app):
    with app.app_context():
        first = _add_machine()
        second = _add_machine(document="DOC-2", row="2", serial="MAT-002")
        update_machine_production_note(_machine_note_payload(first, "Nota prima macchina"))
        update_machine_production_note(_machine_note_payload(second, "Nota seconda macchina"))
        row = create_customer_order(_payload(model_key=_model_key()), ACTOR).righe[0]
        for machine in (first, second):
            update_customer_row(
                row.id, {"version": row.versione, "assignment_changed": True,
                         "id_documento": machine.IdDocumento, "id_riga": machine.IdRiga,
                         "production_note": row.note_produzione or ""},
                ACTOR, can_edit_sales=False, can_edit_production=True, can_assign=True,
            )
        assert row.note_produzione == "Nota seconda macchina"
        assert db.session.get(VenditeNotaProduzioneMacchina, "mat-001").note == "Nota prima macchina"


def test_machine_production_note_survives_stock_transition(app):
    with app.app_context():
        machine = _add_machine(serial="123456")
        update_machine_production_note(_machine_note_payload(machine, "Nota prima dell'ordine"))
        register_closed_machine_stock(machine, closed_by="produzione")
        db.session.delete(machine)
        db.session.flush()
        row = create_customer_order(_payload(model_key=_model_key()), ACTOR).righe[0]
        _assign_test_machine(row, machine)
        assert row.note_produzione == "Nota prima dell'ordine"
        delete_customer_order(row.ordine_cliente_id)
        assert db.session.get(VenditeNotaProduzioneMacchina, "123456").note == "Nota prima dell'ordine"
