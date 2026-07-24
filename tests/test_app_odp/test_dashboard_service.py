from datetime import date
from types import SimpleNamespace

from app_odp.services import dashboard_service as service
from app_odp.services.dashboard_service import (
    _dashboard_carico_ore,
    _dashboard_carico_entro_giorno,
    _dashboard_stato_norm,
    _dashboard_tempo_previsto_minuti_pezzo,
)


def test_dashboard_stato_norm_prefers_runtime_state():
    ordine = SimpleNamespace(
        runtime_row=SimpleNamespace(Stato_odp=" Attivo "),
        StatoOrdine="Pianificata",
        StatoOrdineErp="ERP",
    )

    assert _dashboard_stato_norm(ordine) == "Attivo"


def test_dashboard_stato_norm_falls_back_to_order_and_erp_state():
    assert _dashboard_stato_norm(SimpleNamespace(StatoOrdine=" Pianificata ")) == "Pianificata"
    assert _dashboard_stato_norm(SimpleNamespace(StatoOrdine="", StatoOrdineErp="ERP")) == "ERP"


def test_dashboard_tempo_previsto_minuti_pezzo_reads_active_phase_value():
    ordine = SimpleNamespace(
        runtime_row=None,
        FaseAttiva="2",
        NumFase='["1", "2"]',
        TempoPrevistoLavoraz='["1.5", "2.25"]',
    )

    assert _dashboard_tempo_previsto_minuti_pezzo(ordine) == 2.25


def test_dashboard_tempo_previsto_minuti_pezzo_returns_zero_for_invalid_values():
    ordine = SimpleNamespace(
        runtime_row=None,
        FaseAttiva="1",
        NumFase="",
        TempoPrevistoLavoraz="non-numero",
    )

    assert _dashboard_tempo_previsto_minuti_pezzo(ordine) == 0.0


def test_dashboard_carico_ore_uses_remaining_quantity_without_setup():
    ordine = SimpleNamespace(
        runtime_row=None,
        FaseAttiva="1",
        NumFase='["1"]',
        TempoPrevistoLavoraz='["20"]',
        QtyDaLavorare="400",
        Quantita="400",
        AttrezzaggioAttivo="120",
        TempoAttrezzaggio='["120"]',
    )

    assert _dashboard_carico_ore(ordine) == 133.33


def test_dashboard_carico_entro_giorno_includes_backlog_and_unscheduled(monkeypatch):
    ordini = [
        SimpleNamespace(key="arretrato"),
        SimpleNamespace(key="senza_scadenza"),
        SimpleNamespace(key="entro_finestra"),
        SimpleNamespace(key="fuori_finestra"),
    ]
    dates = {
        "arretrato": date(2026, 7, 20),
        "senza_scadenza": None,
        "entro_finestra": date(2026, 7, 30),
        "fuori_finestra": date(2026, 7, 31),
    }
    hours = {
        "arretrato": 3.0,
        "senza_scadenza": 2.0,
        "entro_finestra": 4.0,
        "fuori_finestra": 10.0,
    }

    monkeypatch.setattr(
        service,
        "_dashboard_data_fine_prevista",
        lambda ordine: dates[ordine.key],
    )
    monkeypatch.setattr(
        service,
        "_dashboard_carico_ore",
        lambda ordine: hours[ordine.key],
    )

    assert _dashboard_carico_entro_giorno(
        ordini,
        date(2026, 7, 30),
    ) == 9.0


def test_dashboard_order_payload_builds_confirmed_ui_fields(monkeypatch):
    ordine = SimpleNamespace(
        CodArt=" ART-1 ",
        DesArt=" Pompa ",
        runtime_row=SimpleNamespace(Utente_operazione=" mario "),
    )
    helper_values = {
        "_dashboard_data_fine_prevista": date(2026, 7, 8),
        "_dashboard_order_key": "DOC1|2|3",
        "_dashboard_order_label": "ORD-1",
        "_dashboard_reparto_attivo": "REP",
        "_dashboard_risorsa_attiva": "R-01",
        "_dashboard_lavorazione_attiva": "Montaggio",
        "_dashboard_stato_norm": "Attivo",
        "_dashboard_fase_attiva": "3",
        "_dashboard_carico_ore": 1.5,
    }
    for name, value in helper_values.items():
        monkeypatch.setattr(service, name, lambda _ordine, value=value: value)
    monkeypatch.setattr(service, "_dashboard_today", lambda: date(2026, 7, 10))

    result = service._dashboard_order_payload(ordine, tipo_criticita="ritardo")

    assert result == {
        "key": "DOC1|2|3",
        "tipo": "ritardo",
        "ordine": "ORD-1",
        "cod_art": "ART-1",
        "descrizione": "Pompa",
        "reparto": "REP",
        "risorsa": "R-01",
        "lavorazione": "Montaggio",
        "stato": "Attivo",
        "fase": "3",
        "operatore": "mario",
        "data_fine_prevista": "2026-07-08",
        "ritardo_giorni": 2,
        "tempo_previsto_ore": 1.5,
        "priorita": "",
    }


def test_dashboard_row_matches_filters_uses_case_insensitive_and_logic():
    row = {
        "reparto": "Montaggio",
        "risorsa": "R-01",
        "lavorazione": "Assemblaggio finale",
        "operatore": "Mario Rossi",
        "cod_art": "ABC-123",
        "descrizione": "Pompa principale",
        "stato": "Attivo",
    }
    filters = {
        "reparto": "MONT",
        "risorsa": "r-0",
        "lavorazione": "semblaggio",
        "operatore": "rossi",
        "articolo": "pompa",
        "stato": "ATT",
    }

    assert service._dashboard_row_matches_filters(row, filters) is True
    assert service._dashboard_row_matches_filters(
        row,
        {"articolo": "abc-123"},
    ) is True

    filters["stato"] = "Chiuso"
    assert service._dashboard_row_matches_filters(row, filters) is False

