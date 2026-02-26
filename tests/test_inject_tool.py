from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import tomllib
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

# importa i modelli reali del progetto
from app_odp.models import InputOdp, ChangeEvent

CONFIG_PATH = Path("app_odp/static/config.toml")

# reparti che al momento risultano attivi nel bridge
TAB_TO_REPARTO = {
    "montaggio": "10",
    "officina": "20",
    "carpenteria": "30",
}


def load_config(path: Path) -> dict:
    with path.open("rb") as f:
        return tomllib.load(f)


def make_engine():
    cfg = load_config(CONFIG_PATH)
    db_path = cfg["Percorsi"]["percorso_db"]
    return create_engine(f"sqlite:///{db_path}")


def build_test_row(tab: str, stato: str = "Pianificata") -> InputOdp:
    reparto = TAB_TO_REPARTO[tab]
    now = datetime.now()
    ts = now.strftime("%Y%m%d%H%M%S")

    id_documento = f"TEST-{tab.upper()}-{ts}"
    id_riga = "1"

    return InputOdp(
        IdDocumento=id_documento,
        IdRiga=id_riga,
        RifRegistraz=f"ODP-TEST-{ts}",
        CodArt=f"ART-TEST-{tab[:3].upper()}",
        DesArt=f"Ordine test {tab}",
        Quantita="1",
        NumFase="1",
        CodLavorazione=json.dumps(["SAL"]),
        CodRisorsaProd=json.dumps([f"SALDATURA"]),
        DataInizioSched=now.strftime("%Y-%m-%d %H:%M:%S"),
        DataFineSched=(now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        GestioneLotto="0",
        GestioneMatricola="0",
        DistintaMateriale=json.dumps([]),
        CodMatricola="",
        StatoRiga="0",
        CodFamiglia="PRO",
        CodMacrofamiglia="IMP",
        CodMagPrincipale="0",
        CodReparto=json.dumps([reparto]),
        TempoPrevistoLavoraz="60",
        StatoOrdine=stato,
        CodClassifTecnica="",
        CodTipoDoc="ODP",
        FaseAttiva="1",
        Note="INIEZIONE_TEST_UI",
    )


def build_test_event(row: InputOdp, tab: str) -> ChangeEvent:
    reparto = TAB_TO_REPARTO[tab]
    payload = [f"{row.IdDocumento},{row.IdRiga}"]
    scope = [reparto]

    return ChangeEvent(
        topic="nuovo_ordine",
        scope=json.dumps(scope),
        payload_json=json.dumps(payload),
    )


def inject(tab: str, stato: str = "Pianificata") -> None:
    if tab not in TAB_TO_REPARTO:
        raise ValueError(
            f"Tab non valida: {tab}. Usa una di: {', '.join(TAB_TO_REPARTO)}"
        )

    engine = make_engine()
    row = build_test_row(tab=tab, stato=stato)
    event = build_test_event(row=row, tab=tab)

    id_documento = row.IdDocumento
    id_riga = row.IdRiga
    stato = row.StatoOrdine

    with Session(engine) as session:
        try:
            session.add(row)
            session.add(event)
            session.commit()
        except Exception:
            session.rollback()
            raise

    print("Iniezione completata")
    print(f"IdDoc:  {id_documento}")
    print(f"IdRiga: {id_riga}")
    print(f"Stato:  {stato}")


if __name__ == "__main__":
    tab = sys.argv[1] if len(sys.argv) > 1 else "officina"
    stato = sys.argv[2] if len(sys.argv) > 2 else "Pianificata"
    inject(tab=tab, stato=stato)
