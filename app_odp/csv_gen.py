import csv
import json
from pathlib import Path
from models import ErpOutbox
from datetime import datetime
from zoneinfo import ZoneInfo


def export_pending_outbox_rows(session, out_dir: str):
    rows = (
        session.query(ErpOutbox)
        .filter_by(kind="consuntivo_fase", status="pending")
        .order_by(ErpOutbox.outbox_id)
        .all()
    )

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    for row in rows:
        payload = json.loads(row.payload_json)

        lotti_csv = ";".join(
            f"{x['CodArt']}|{x['RifLottoAlfa']}|{x['Quantita']}|{x['Esito']}"
            for x in payload.get("lotti", [])
        )

        filename = (
            f"{payload['id_documento']}_{payload['id_riga']}_fase_{payload['fase']}.csv"
        )
        path = Path(out_dir) / filename

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(
                [
                    "IdDocumento",
                    "IdRiga",
                    "CodArt",
                    "Fase",
                    "QuantitaOK",
                    "QuantitaKO",
                    "TempoFunzionamento",
                    "Lotti",
                ]
            )
            writer.writerow(
                [
                    payload["id_documento"],
                    payload["id_riga"],
                    payload["cod_art"],
                    payload["fase"],
                    payload["quantita_ok"],
                    payload["quantita_ko"],
                    payload["tempo_funzionamento"],
                    lotti_csv,
                ]
            )

        row.status = "exported"
        row.exported_at = datetime.now(ZoneInfo("Europe/Rome")).isoformat(
            timespec="seconds"
        )

    session.commit()
