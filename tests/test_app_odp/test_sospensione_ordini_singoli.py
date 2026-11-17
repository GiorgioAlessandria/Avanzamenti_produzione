from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from app_odp.odp_output import txt_generator
from app_odp.services import erp_export_service
from app_odp.services.ordini_runtime_service import (
    _accumulate_runtime_until,
    _consuntiva_intervallo_singolo,
)


def test_sospensioni_e_chiusura_consuntivano_intervalli_distinti():
    runtime = SimpleNamespace(Tempo_funzionamento="2.8", data_ultima_attivazione=None)
    for apertura, fine, secondi, ore in [
        ("10:00", "11:00", 3600, "1"),
        ("12:10", "12:30", 1200, "0.33"),
        ("15:00", "16:30", 5400, "1.5"),
    ]:
        runtime.data_ultima_attivazione = f"2026-11-17T{apertura}:00+01:00"
        end = datetime.fromisoformat(f"2026-11-17T{fine}:00+01:00")
        assert _consuntiva_intervallo_singolo(runtime, end) == (secondi, 0, ore)
        assert runtime.data_ultima_attivazione is None
        assert runtime.Tempo_funzionamento == "0"


def test_singolo_sottrae_fermo_e_accumulo_gruppi_resta_disponibile():
    start = "2026-11-17T12:10:00+01:00"
    end = datetime.fromisoformat("2026-11-17T12:12:00+01:00")
    runtime = SimpleNamespace(data_ultima_attivazione=start, Tempo_funzionamento="2")
    assert _consuntiva_intervallo_singolo(runtime, end, 2) == (120, 120, "0")
    runtime = SimpleNamespace(data_ultima_attivazione=start, Tempo_funzionamento="2")
    assert _accumulate_runtime_until(runtime, end) == 120
    assert runtime.Tempo_funzionamento == "2.03"


def test_avp_di_sospensione_contiene_solo_tes_e_709():
    ordine = SimpleNamespace(
        IdDocumento="10", IdRiga="1", RifRegistraz="001-2026-000001",
        CodArt="ART", DesArt="Articolo", NumFase="1", NumProgrRiga="1",
        CodTipoDoc="700", RisorsaAttiva="R1", CodMagPrincipale="M1", VarianteArt="",
    )
    with patch.multiple(
        erp_export_service,
        _current_username=lambda: "operatore",
        _queue_phase_export=lambda ordine, fase, payload: payload,
    ):
        payload = erp_export_service._queue_suspension_export(
            ordine, "1", "0.33", "Pausa", datetime.fromisoformat("2026-11-17T12:30:00+01:00")
        )

    payload["is_last_phase"] = True
    payload["distinta_base"] = [{"CodArt": "COMP"}]
    rows = [line.split(";") for line in txt_generator([{"payload": payload}])]
    assert [row[0] for row in rows] == ["TES", "RIG"]
    assert rows[1][4] == "709"
    assert rows[1][11] == "0"
    assert rows[1][17] == "0.33"

    payload["kind"] = "consuntivo_fase"
    rows = [line.split(";") for line in txt_generator([{"payload": payload}])]
    assert [row[4] for row in rows] == ["", "701", "709", "703"]
