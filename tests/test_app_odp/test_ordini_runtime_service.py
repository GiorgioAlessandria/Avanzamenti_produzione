from types import SimpleNamespace

from app_odp.services.ordini_runtime_service import _runtime_snapshot


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

