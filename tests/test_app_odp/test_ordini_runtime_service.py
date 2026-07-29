from types import SimpleNamespace

import pytest

from app_odp.services.ordini_runtime_service import (
    _apply_stop_minutes_to_runtime,
    _runtime_snapshot,
)


def test_runtime_snapshot_normalizes_expected_runtime_fields():
    stato = SimpleNamespace(
        Stato_odp=" Attivo ",
        FaseAttiva=" 2 ",
        Data_in_carico="2026-07-09T08:00:00",
        data_ultima_attivazione="2026-07-09T09:00:00",
        Tempo_funzionamento=" 120 ",
        QtyDaLavorare=" 5 ",
        Utente_operazione=" mario ",
        RifOrdinePrinc=" ORD-1 ",
    )

    assert _runtime_snapshot(stato) == {
        "stato_odp": "Attivo",
        "fase": "2",
        "data_in_carico": "2026-07-09T08:00:00",
        "data_ultima_attivazione": "2026-07-09T09:00:00",
        "tempo_funzionamento": "120",
        "qty_da_lavorare": "5",
        "utente_operazione": "mario",
        "rif_ordine_princ": "ORD-1",
    }


def test_runtime_snapshot_returns_blank_fields_for_missing_runtime_values():
    assert _runtime_snapshot(SimpleNamespace()) == {
        "stato_odp": "",
        "fase": "",
        "data_in_carico": "",
        "data_ultima_attivazione": "",
        "tempo_funzionamento": "",
        "qty_da_lavorare": "",
        "utente_operazione": "",
        "rif_ordine_princ": "",
    }


def test_stop_minutes_over_measured_interval_are_reported_to_the_user():
    stato = SimpleNamespace(Tempo_funzionamento="10")

    with pytest.raises(ValueError, match=r"Tempo massimo disponibile: 00:30:00"):
        _apply_stop_minutes_to_runtime(
            stato,
            minuti_non_funzionamento=120,
            max_removable_seconds=1800,
        )

    assert stato.Tempo_funzionamento == "10"
