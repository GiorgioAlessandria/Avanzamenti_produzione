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

from app_odp.services.order_helpers import _parse_phase_list
from app_odp.services.kpi_service import (
    KPI_CLOSURE_ACTIONS, _kpi_history, _kpi_is_partial,
    _kpi_is_order_completed as _is_order_completed,
    _kpi_macchine_prodotte as _macchine_prodotte_for_log,
    _kpi_matricola_for_log,
    _kpi_event_is_eligible as _event_is_eligible,
    _kpi_jsonish_list as _jsonish_list,
    _kpi_active_value_from_list as _active_value_from_list,
    _kpi_reparto_for_log as _reparto_for_log,
    _kpi_risorsa_for_log as _risorsa_for_log,
    _kpi_lavorazione_for_log as _lavorazione_for_log,
    _kpi_tempo_previsto_ore as _tempo_previsto_ore,
    _kpi_tempo_reale_ore as _tempo_reale_ore,
    _kpi_closed_at as _closed_at,
    _kpi_data_fine_prevista as _data_fine_prevista,
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


def _fase_to_int(value) -> int | None:
    raw = _norm_text(value)
    if not raw:
        return None

    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _first_code_from_cell(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    values = _jsonish_list(raw)
    if values:
        return _norm_text(values[0])

    return raw


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
        "fasi_consuntivate": 0,
        "ordini_chiusi": 0,
        "ordini_in_ritardo": 0,
        "giorni_ritardo_totali": 0.0,
        "tempo_previsto_totale": 0.0,
        "tempo_reale_totale": 0.0,
        "tempo_medio_ordine": 0.0,
        "tempo_medio_fase": 0.0,
        "macchine_prodotte": 0.0,
        "matricole_prodotte": set(),
        "rows": [],
    }


def _apply_to_bucket(bucket: dict, row: dict) -> None:
    bucket["fasi_consuntivate"] += 1
    bucket["tempo_previsto_totale"] += row["tempo_previsto_ore"]
    bucket["tempo_reale_totale"] += row["tempo_reale_ore"]
    bucket["rows"].append(row)

    matricola = row.get("matricola")
    if row.get("macchine_prodotte") and matricola:
        bucket["matricole_prodotte"].add(matricola)
        bucket["macchine_prodotte"] = float(len(bucket["matricole_prodotte"]))

    if not row.get("is_order_completed"):
        return

    bucket["ordini_chiusi"] += 1

    if row["ritardo_giorni"] > 0:
        bucket["ordini_in_ritardo"] += 1
        bucket["giorni_ritardo_totali"] += row["ritardo_giorni"]


def _finalize_bucket(
    bucket: dict, *, snapshot_month: str, period_start: date, period_end: date
) -> dict:
    ordini = int(bucket["ordini_chiusi"] or 0)
    fasi = int(bucket["fasi_consuntivate"] or 0)
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
        "tempo_medio_ordine": None,
        "tempo_medio_fase": round(tempo_reale / fasi, 2) if fasi else 0.0,
        "payload_json": json.dumps(
            {
                "fasi_consuntivate": fasi,
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
        .filter(OdpRuntimeLog.Azione.in_(KPI_CLOSURE_ACTIONS))
        .order_by(OdpRuntimeLog.EventAt.asc(), OdpRuntimeLog.log_id.asc())
        .all()
    )

    _order_starts, phase_quantities = _kpi_history(rows)
    buckets: dict[tuple[str, str], dict] = {}

    def bucket(scope_type: str, scope_code: str) -> dict:
        scope_code = _norm_text(scope_code) or "-"
        key = (scope_type, scope_code)
        if key not in buckets:
            buckets[key] = _new_bucket(scope_type, scope_code)
        return buckets[key]

    bucket("global", "*")
    eligible_count = 0

    for rt, il in rows:
        if not _event_is_eligible(rt, il) or _kpi_is_partial(rt, il):
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

        quantity_key = (_norm_text(rt.OperationGroupId), _norm_text(rt.IdDocumento), _norm_text(rt.IdRiga))
        tempo_previsto = _tempo_previsto_ore(il, quantita=phase_quantities.get(quantity_key))
        tempo_reale = _tempo_reale_ore(rt, il)
        is_order_completed = _is_order_completed(rt, il)
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
            "matricola": _kpi_matricola_for_log(rt, il),
            "tempo_previsto_ore": tempo_previsto,
            "tempo_reale_ore": tempo_reale,
            "ritardo_giorni": ritardo_giorni,
            "is_order_completed": is_order_completed,
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

    valid_keys = {(record["scope_type"], record["scope_code"]) for record in records}
    for old_row in ProductionKpiSnapshot.query.filter_by(snapshot_month=month).all():
        if (old_row.scope_type, old_row.scope_code) not in valid_keys:
            db.session.delete(old_row)

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
