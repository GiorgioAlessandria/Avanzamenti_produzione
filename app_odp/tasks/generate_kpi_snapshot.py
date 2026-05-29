from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import and_

from app_odp.app import create_app
from app_odp.models import (
    db,
    InputOdpLog,
    OdpRuntimeLog,
    ProductionKpiSnapshot,
)

ROME_TZ = ZoneInfo("Europe/Rome")


def _norm_text(value) -> str:
    return str(value or "").strip()


def _safe_float(value) -> float:
    raw = _norm_text(value).replace(",", ".")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _positive_float(value) -> float:
    out = _safe_float(value)
    return out if out > 0 else 0.0


def _macchine_prodotte_for_log(rt: OdpRuntimeLog, il: InputOdpLog | None) -> float:
    """
    Numero macchine prodotte nello snapshot mensile.

    Conta 1 macchina per ogni chiusura_finale valida.
    Non usa QuantitaConforme/Quantita perché quei campi possono rappresentare pezzi,
    quantità o componenti, non necessariamente macchine finite.
    """
    azione = _norm_text(getattr(rt, "Azione", "")).lower()

    if azione != "chiusura_finale":
        return 0.0

    return 1.0


def _parse_date(value) -> date | None:
    raw = _norm_text(value)
    if not raw:
        return None

    raw = raw[:19]

    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(raw).date()
    except Exception:
        return None


def _parse_datetime(value) -> datetime | None:
    raw = _norm_text(value)
    if not raw:
        return None

    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ROME_TZ)
        return dt.astimezone(ROME_TZ)
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(raw[:19], fmt)
            return dt.replace(tzinfo=ROME_TZ)
        except ValueError:
            pass

    return None


def _jsonish_list(value) -> list[str]:
    raw = _norm_text(value)
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        return [raw]

    if not isinstance(parsed, list):
        parsed = [parsed]

    return [_norm_text(x) for x in parsed if _norm_text(x)]


def _fase_to_int(value) -> int | None:
    raw = _norm_text(value)
    if not raw:
        return None

    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _parse_phase_list(value) -> list[str]:
    values = _jsonish_list(value)
    if values:
        return values

    phase_int = _fase_to_int(value)
    if phase_int is not None and phase_int > 0:
        return [str(i) for i in range(1, phase_int + 1)]

    return []


def _active_value_from_list(raw_values, raw_phases, fase: str) -> str:
    values = _jsonish_list(raw_values)
    phases = _parse_phase_list(raw_phases)
    fase = _norm_text(fase)

    if not values:
        return ""

    if phases and len(phases) == len(values):
        for phase, value in zip(phases, values):
            if _norm_text(phase) == fase:
                return _norm_text(value)

    fase_int = _fase_to_int(fase)
    if fase_int is not None:
        idx = fase_int - 1
        if 0 <= idx < len(values):
            return _norm_text(values[idx])

    if len(values) == 1:
        return _norm_text(values[0])

    return ""


def _first_code_from_cell(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    values = _jsonish_list(raw)
    if values:
        return _norm_text(values[0])

    return raw


def _event_is_eligible(rt: OdpRuntimeLog, il: InputOdpLog | None = None) -> bool:
    azione = _norm_text(getattr(rt, "Azione", "")).lower()
    topic = _norm_text(getattr(rt, "Topic", "")).lower()
    motivo = _norm_text(getattr(rt, "Motivo", "")).lower()
    payload = _norm_text(getattr(rt, "PayloadJson", "")).lower()

    if azione not in {"chiusura_finale", "chiusura_macchina"}:
        return False

    if "eliminato_gestionale" in azione:
        return False

    if "eliminato_gestionale" in topic:
        return False

    if "eliminato dal gestionale" in motivo:
        return False

    if "eliminato_gestionale" in payload:
        return False

    if il is not None:
        chiusura_parziale = _norm_text(getattr(il, "ChiusuraParziale", "")).lower()
        if chiusura_parziale in {"1", "true", "si", "sì", "yes"}:
            return False

    return True


def _reparto_for_log(rt: OdpRuntimeLog, il: InputOdpLog | None) -> str:
    if il is not None:
        fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(
            getattr(il, "FaseAttiva", "")
        )
        value = _active_value_from_list(
            getattr(il, "CodReparto", ""),
            getattr(il, "NumFase", ""),
            fase,
        )
        return _first_code_from_cell(value) or _first_code_from_cell(
            getattr(il, "CodReparto", "")
        )

    return _first_code_from_cell(getattr(rt, "CodReparto", ""))


def _risorsa_for_log(il: InputOdpLog | None) -> str:
    if il is None:
        return ""

    if _norm_text(getattr(il, "RisorsaAttiva", "")):
        return _norm_text(getattr(il, "RisorsaAttiva", ""))

    fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(
        getattr(il, "FaseAttiva", "")
    )

    value = _active_value_from_list(
        getattr(il, "CodRisorsaProd", ""),
        getattr(il, "NumFase", ""),
        fase,
    )

    return _first_code_from_cell(value)


def _lavorazione_for_log(il: InputOdpLog | None) -> str:
    if il is None:
        return ""

    if _norm_text(getattr(il, "LavorazioneAttiva", "")):
        return _norm_text(getattr(il, "LavorazioneAttiva", ""))

    fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(
        getattr(il, "FaseAttiva", "")
    )

    value = _active_value_from_list(
        getattr(il, "CodLavorazione", ""),
        getattr(il, "NumFase", ""),
        fase,
    )

    return _first_code_from_cell(value)


def _tempo_previsto_ore(il: InputOdpLog | None) -> float:
    if il is None:
        return 0.0

    fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(
        getattr(il, "FaseAttiva", "")
    )

    raw = _active_value_from_list(
        getattr(il, "TempoPrevistoLavoraz", ""),
        getattr(il, "NumFase", ""),
        fase,
    )

    value = _safe_float(raw)

    if value > 0:
        return value

    return _safe_float(getattr(il, "TempoPrevistoLavoraz", ""))


def _tempo_reale_ore(rt: OdpRuntimeLog, il: InputOdpLog | None) -> float:
    if il is not None:
        value = _safe_float(getattr(il, "TempoFunzionamentoFinale", ""))
        if value > 0:
            return value

    value = _safe_float(getattr(rt, "TempoFunzionamentoPost", ""))
    if value > 0:
        return value

    elapsed_seconds = _safe_float(getattr(rt, "ElapsedSeconds", ""))
    if elapsed_seconds > 0:
        return elapsed_seconds / 3600.0

    return 0.0


def _closed_at(rt: OdpRuntimeLog, il: InputOdpLog | None) -> datetime | None:
    if il is not None:
        dt = _parse_datetime(getattr(il, "ClosedAt", ""))
        if dt is not None:
            return dt

    return _parse_datetime(getattr(rt, "EventAt", ""))


def _data_fine_prevista(il: InputOdpLog | None) -> date | None:
    if il is None:
        return None

    return _parse_date(getattr(il, "DataFineSched", ""))


def _month_bounds(month: str) -> tuple[date, date]:
    year, month_num = month.split("-", 1)
    start = date(int(year), int(month_num), 1)

    if start.month == 12:
        end = date(start.year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(start.year, start.month + 1, 1) - timedelta(days=1)

    return start, end


def _previous_month(today: date | None = None) -> str:
    today = today or datetime.now(ROME_TZ).date()
    first_this_month = date(today.year, today.month, 1)
    last_previous_month = first_this_month - timedelta(days=1)

    return f"{last_previous_month.year:04d}-{last_previous_month.month:02d}"


def _new_bucket(scope_type: str, scope_code: str) -> dict:
    return {
        "scope_type": scope_type,
        "scope_code": scope_code,
        "ordini_chiusi": 0,
        "ordini_in_ritardo": 0,
        "giorni_ritardo_totali": 0.0,
        "tempo_previsto_totale": 0.0,
        "tempo_reale_totale": 0.0,
        "tempo_medio_ordine": 0.0,
        "tempo_medio_fase": 0.0,
        "macchine_prodotte": 0.0,
        "rows": [],
    }


def _apply_to_bucket(bucket: dict, row: dict) -> None:
    bucket["ordini_chiusi"] += 1
    bucket["macchine_prodotte"] += float(row.get("macchine_prodotte", 0.0) or 0.0)

    if row["ritardo_giorni"] > 0:
        bucket["ordini_in_ritardo"] += 1
        bucket["giorni_ritardo_totali"] += row["ritardo_giorni"]

    bucket["tempo_previsto_totale"] += row["tempo_previsto_ore"]
    bucket["tempo_reale_totale"] += row["tempo_reale_ore"]
    bucket["rows"].append(row)


def _finalize_bucket(
    bucket: dict, *, snapshot_month: str, period_start: date, period_end: date
) -> dict:
    ordini = int(bucket["ordini_chiusi"] or 0)
    ritardi = int(bucket["ordini_in_ritardo"] or 0)
    macchine_prodotte = float(bucket.get("macchine_prodotte", 0.0) or 0.0)

    tempo_previsto = float(bucket["tempo_previsto_totale"] or 0.0)
    tempo_reale = float(bucket["tempo_reale_totale"] or 0.0)
    scostamento = tempo_reale - tempo_previsto

    return {
        "snapshot_month": snapshot_month,
        "scope_type": bucket["scope_type"],
        "scope_code": bucket["scope_code"],
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "ordini_chiusi": ordini,
        "ordini_in_ritardo": ritardi,
        "macchine_prodotte": round(macchine_prodotte, 2),
        "percentuale_ritardo": round((ritardi / ordini) * 100, 2) if ordini else 0.0,
        "giorni_medi_ritardo": round(bucket["giorni_ritardo_totali"] / ritardi, 2)
        if ritardi
        else 0.0,
        "tempo_previsto_totale": round(tempo_previsto, 2),
        "tempo_reale_totale": round(tempo_reale, 2),
        "scostamento_totale": round(scostamento, 2),
        "scostamento_percentuale": round((scostamento / tempo_previsto) * 100, 2)
        if tempo_previsto > 0
        else 0.0,
        "tempo_medio_ordine": round(tempo_reale / ordini, 2) if ordini else 0.0,
        "tempo_medio_fase": round(tempo_reale / ordini, 2) if ordini else 0.0,
        "payload_json": json.dumps(
            {
                "details_count": len(bucket["rows"]),
                "generated_at": datetime.now(ROME_TZ).isoformat(timespec="seconds"),
            },
            ensure_ascii=False,
        ),
    }


def _upsert_snapshot(record: dict, *, created_by: str) -> None:
    row = ProductionKpiSnapshot.query.filter_by(
        snapshot_month=record["snapshot_month"],
        scope_type=record["scope_type"],
        scope_code=record["scope_code"],
    ).first()

    if row is None:
        row = ProductionKpiSnapshot(
            snapshot_month=record["snapshot_month"],
            scope_type=record["scope_type"],
            scope_code=record["scope_code"],
        )
        db.session.add(row)

    row.period_start = record["period_start"]
    row.period_end = record["period_end"]
    row.ordini_chiusi = record["ordini_chiusi"]
    row.ordini_in_ritardo = record["ordini_in_ritardo"]
    row.percentuale_ritardo = record["percentuale_ritardo"]
    row.macchine_prodotte = record["macchine_prodotte"]
    row.giorni_medi_ritardo = record["giorni_medi_ritardo"]
    row.tempo_previsto_totale = record["tempo_previsto_totale"]
    row.tempo_reale_totale = record["tempo_reale_totale"]
    row.scostamento_totale = record["scostamento_totale"]
    row.scostamento_percentuale = record["scostamento_percentuale"]
    row.tempo_medio_ordine = record["tempo_medio_ordine"]
    row.tempo_medio_fase = record["tempo_medio_fase"]
    row.payload_json = record["payload_json"]
    row.created_at = datetime.now(ROME_TZ).isoformat(timespec="seconds")
    row.created_by = created_by


def generate_snapshot_for_month(
    month: str, *, created_by: str = "scheduled_task"
) -> dict:
    period_start, period_end = _month_bounds(month)

    period_start_dt = datetime.combine(period_start, datetime.min.time()).replace(
        tzinfo=ROME_TZ
    )
    period_end_dt = datetime.combine(period_end, datetime.max.time()).replace(
        tzinfo=ROME_TZ
    )

    rows = (
        db.session.query(OdpRuntimeLog, InputOdpLog)
        .outerjoin(
            InputOdpLog,
            and_(
                InputOdpLog.OperationGroupId == OdpRuntimeLog.OperationGroupId,
                InputOdpLog.IdDocumento == OdpRuntimeLog.IdDocumento,
                InputOdpLog.IdRiga == OdpRuntimeLog.IdRiga,
            ),
        )
        .filter(OdpRuntimeLog.Azione.in_(["chiusura_finale", "chiusura_macchina"]))
        .order_by(OdpRuntimeLog.EventAt.asc(), OdpRuntimeLog.log_id.asc())
        .all()
    )

    buckets: dict[tuple[str, str], dict] = {}

    def bucket(scope_type: str, scope_code: str) -> dict:
        scope_code = _norm_text(scope_code) or "-"
        key = (scope_type, scope_code)
        if key not in buckets:
            buckets[key] = _new_bucket(scope_type, scope_code)
        return buckets[key]

    eligible_count = 0

    for rt, il in rows:
        if not _event_is_eligible(rt, il):
            continue

        closed_at = _closed_at(rt, il)
        if closed_at is None:
            continue

        if closed_at < period_start_dt or closed_at > period_end_dt:
            continue

        reparto = _reparto_for_log(rt, il)
        risorsa = _risorsa_for_log(il)
        lavorazione = _lavorazione_for_log(il)
        operatore = _norm_text(getattr(rt, "UtenteOperazione", ""))
        articolo = (
            _norm_text(getattr(il, "CodArt", ""))
            if il
            else _norm_text(getattr(rt, "CodArt", ""))
        )

        tempo_previsto = _tempo_previsto_ore(il)
        tempo_reale = _tempo_reale_ore(rt, il)
        macchine_prodotte = _macchine_prodotte_for_log(rt, il)

        data_fine_prevista = _data_fine_prevista(il)
        ritardo_giorni = 0

        if data_fine_prevista and closed_at.date() > data_fine_prevista:
            ritardo_giorni = (closed_at.date() - data_fine_prevista).days

        row = {
            "operation_group_id": _norm_text(getattr(rt, "OperationGroupId", "")),
            "id_documento": _norm_text(getattr(rt, "IdDocumento", "")),
            "id_riga": _norm_text(getattr(rt, "IdRiga", "")),
            "data_chiusura": closed_at.date().isoformat(),
            "reparto": reparto,
            "risorsa": risorsa,
            "lavorazione": lavorazione,
            "operatore": operatore,
            "articolo": articolo,
            "macchine_prodotte": macchine_prodotte,
            "tempo_previsto_ore": tempo_previsto,
            "tempo_reale_ore": tempo_reale,
            "ritardo_giorni": ritardo_giorni,
        }

        eligible_count += 1

        for scope_type, scope_code in (
            ("global", "*"),
            ("reparto", reparto),
            ("risorsa", risorsa),
            ("lavorazione", lavorazione),
            ("operatore", operatore),
            ("articolo", articolo),
        ):
            _apply_to_bucket(bucket(scope_type, scope_code), row)

    records = [
        _finalize_bucket(
            b,
            snapshot_month=month,
            period_start=period_start,
            period_end=period_end,
        )
        for b in buckets.values()
    ]

    for record in records:
        _upsert_snapshot(record, created_by=created_by)

    db.session.commit()

    return {
        "month": month,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "eligible_events": eligible_count,
        "snapshots_written": len(records),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--month",
        default="previous",
        help="Mese da generare in formato YYYY-MM oppure 'previous'.",
    )
    args = parser.parse_args()

    month = _previous_month() if args.month == "previous" else args.month

    app = create_app()

    with app.app_context():
        result = generate_snapshot_for_month(month)

    print(json.dumps(result, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
