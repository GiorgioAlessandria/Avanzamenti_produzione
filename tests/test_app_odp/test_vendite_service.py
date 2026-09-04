from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from app_odp.models import InputOdp, OdpDistintaMancante, db
from app_odp.services.ordini_distinta_mancante_service import save_missing_components

from app_odp.services.vendite_service import (
    _build_vendite_payload,
    _parse_suspension_cause,
    build_vendite_payload,
)


def _order(
    row,
    *,
    model="MODELLO-1",
    variant="",
    serial="MAT-001",
    phase="1",
    state="Pianificata",
    description="Macchina prova",
):
    return SimpleNamespace(
        IdDocumento="DOC",
        IdRiga=str(row),
        RifRegistraz="2026.1.10",
        NumProgrRiga=str(row),
        CodArt=model,
        VarianteArt=variant,
        DesArt=description,
        CodMatricola=serial,
        FaseAttiva=phase,
        StatoOrdine=state,
    )


def test_planned_filter_removes_machines_models_columns_and_totals():
    orders = [
        _order(1, model="SOLO-PIANIFICATO", serial="PLAN", state="pianificato"),
        _order(2, serial="ACTIVE", state="Attivo"),
        _order(3, serial="SUSPENDED", state="Sospeso"),
        _order(4, serial="DEFAULT", state=""),
    ]
    payload = _build_vendite_payload(orders, include_planned=False)
    assert payload["total_machines"] == 2
    assert {m["serial_number"] for m in payload["machines"]} == {"ACTIVE", "SUSPENDED"}
    assert {c["state"] for c in payload["columns"]} == {"Attivo", "In Sospeso"}
    assert [m["model_code"] for m in payload["models"]] == ["MODELLO-1"]
    assert payload["models"][0]["total"] == 2
    assert _build_vendite_payload(orders, include_planned=True)["total_machines"] == 4


def test_build_vendite_payload_groups_by_model_phase_and_state():
    orders = [
        _order(1, state="Attivo", serial="MAT-001"),
        _order(2, state="Sospeso", serial="MAT-002"),
        _order(3, phase="2", state="Pianificata", serial="MAT-003"),
        _order(
            4,
            model="MODELLO-2",
            variant="V2",
            phase="2.0",
            state="ATTIVA",
            serial="MAT-004",
        ),
        _order(5, model="MODELLO-2", variant="V2", state="Chiusa"),
    ]

    payload = _build_vendite_payload(
        orders,
        {
            ("DOC", "1"): "Manca componente",
            ("DOC", "2"): "Attesa materiale",
        },
        customer_assignments={
            ("order", "DOC", "1"): {
                "customer_order": "OC-123",
                "production_note": "Configurazione speciale",
            },
        },
        generated_at="2026-08-05T12:00:00+02:00",
    )

    assert payload["total_machines"] == 4
    assert payload["columns"] == [
        {"phase": "1", "state": "Pianificata"},
        {"phase": "1", "state": "Attivo"},
        {"phase": "1", "state": "In Sospeso"},
        {"phase": "2", "state": "Pianificata"},
        {"phase": "2", "state": "Attivo"},
        {"phase": "2", "state": "In Sospeso"},
    ]
    assert payload["models"] == [
        {
            "model_code": "MODELLO-1",
            "variant": "",
            "description": "Macchina prova",
            "total": 3,
            "counts": [0, 1, 1, 1, 0, 0],
        },
        {
            "model_code": "MODELLO-2",
            "variant": "V2",
            "description": "Macchina prova",
            "total": 1,
            "counts": [0, 0, 0, 0, 1, 0],
        },
    ]
    assert payload["machines"][0]["last_suspension_cause"] == "Manca componente"
    assert payload["machines"][0]["customer_order"] == "OC-123"
    assert payload["machines"][0]["production_note"] == "Configurazione speciale"
    assert payload["machines"][1]["customer_order"] == ""
    assert payload["machines"][1]["state"] == "In Sospeso"
    assert payload["machines"][1]["last_suspension_cause"] == "Attesa materiale"
    assert all(row["missing_components"] == [] for row in payload["machines"])


def test_missing_components_follow_saved_residue_and_current_machine_phase():
    app = Flask(__name__)
    app.config.update(SQLALCHEMY_DATABASE_URI="sqlite://", SQLALCHEMY_TRACK_MODIFICATIONS=False)
    db.init_app(app)
    with app.app_context():
        # Solo le due tabelle usate; nessun database di produzione o log.
        InputOdp.__table__.create(db.engine)
        OdpDistintaMancante.__table__.create(db.engine)
        db.session.add_all([InputOdp(IdDocumento="DOC", IdRiga="1"),
                            InputOdp(IdDocumento="DOC", IdRiga="2"),
                            InputOdp(IdDocumento="ALTRO", IdRiga="1")])
        db.session.flush()
        machine = _order(1, phase="1.0", state="In Sospeso")
        other_machine = _order(2, serial="MAT-002")
        parts = [
            {"CodArt": "COMP-01", "VarianteArt": "V1", "DesArt": "Motore mancante", "Quantita": 1},
            {"CodArt": "COMP-02", "DesArt": "", "Quantita": 2},
        ]
        save_missing_components(machine, "1", parts)
        save_missing_components(machine, "2", [{"CodArt": "ALTRA-FASE", "Quantita": 1}])
        save_missing_components(SimpleNamespace(IdDocumento="ALTRO", IdRiga="1"), "1",
                                [{"CodArt": "ALTRO-ORDINE", "Quantita": 1}])
        db.session.commit()
        with patch("app_odp.services.vendite_service.load_machine_orders", return_value=[machine, other_machine]), \
             patch("app_odp.services.vendite_service.load_stock_machine_orders", return_value=[]), \
             patch("app_odp.services.vendite_service._latest_suspension_causes", return_value={("DOC", "1"): "Attesa materiale"}), \
             patch("app_odp.services.vendite_service._customer_assignments", return_value={}), \
             patch("app_odp.services.vendite_service._packaging_confirmations", return_value={}), \
             patch("app_odp.services.vendite_service.VenditeNotaProduzioneMacchina"):
            row, other = build_vendite_payload()["machines"]
            assert row["missing_components"] == [
                {"code": "COMP-01", "variant": "V1", "description": "Motore mancante"},
                {"code": "COMP-02", "variant": "", "description": ""},
            ]
            assert row["last_suspension_cause"] == "Attesa materiale"
            assert row["production_note"] == ""
            assert other["missing_components"] == []
            save_missing_components(machine, "1", parts[1:])
            db.session.commit()
            assert build_vendite_payload()["machines"][0]["missing_components"] == [
                {"code": "COMP-02", "variant": "", "description": ""},
            ]
            save_missing_components(machine, "1", [])
            db.session.commit()
            row = build_vendite_payload()["machines"][0]
            assert row["missing_components"] == []
            assert row["last_suspension_cause"] == "Attesa materiale"
        db.session.remove()


def test_parse_suspension_cause_reads_only_causale_segment():
    assert (
        _parse_suspension_cause(
            "Sospensione ordine | Causale: Materiale mancante | "
            "Tempo non funzionamento minuti: 10"
        )
        == "Materiale mancante"
    )
    assert _parse_suspension_cause("Sospensione ordine") == ""
