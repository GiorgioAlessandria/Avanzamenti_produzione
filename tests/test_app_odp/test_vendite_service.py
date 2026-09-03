from types import SimpleNamespace

from app_odp.services.vendite_service import (
    _build_vendite_payload,
    _parse_suspension_cause,
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


def test_parse_suspension_cause_reads_only_causale_segment():
    assert (
        _parse_suspension_cause(
            "Sospensione ordine | Causale: Materiale mancante | "
            "Tempo non funzionamento minuti: 10"
        )
        == "Materiale mancante"
    )
    assert _parse_suspension_cause("Sospensione ordine") == ""
