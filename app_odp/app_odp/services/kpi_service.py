from __future__ import annotations

from datetime import date, datetime
import json
import math

from app_odp.services.order_helpers import (
    ROME_TZ, _norm_text, _safe_float, _fase_to_int, _parse_phase_list,
    _first_code_from_cell,
)

KPI_CLOSURE_ACTIONS = (
    "chiusura_finale", "chiusura_macchina",
    "chiusura_parziale", "chiusura_parziale_macchina",
)


def _kpi_is_order_completed(
    rt, il = None
) -> bool:
    stato = (
        _norm_text(getattr(il, "StatoOrdinePost", "")) if il is not None else ""
    ) or _norm_text(getattr(rt, "StatoOrdinePost", "")) or _norm_text(
        getattr(rt, "StatoOdpPost", "")
    )
    stato = stato.lower()
    return not _kpi_is_partial(rt, il) and ("chius" in stato or "terminat" in stato)


def _kpi_matricola_for_log(rt, il) -> str:
    matricola = _norm_text(getattr(il, "CodMatricola", ""))
    if not matricola:
        try:
            payload = json.loads(getattr(rt, "PayloadJson", "") or "{}")
        except (TypeError, ValueError):
            payload = {}
        if isinstance(payload, dict):
            matricola = _norm_text(payload.get("matricola"))
    return matricola


def _kpi_macchine_prodotte(rt, il) -> float:
    """A completed phase identifies a machine by its serial, regardless of order state."""
    return float(
        _kpi_event_is_eligible(rt, il)
        and not _kpi_is_partial(rt, il)
        and bool(_kpi_matricola_for_log(rt, il))
    )


def _kpi_parse_date(value) -> date | None:
    raw = _norm_text(value)
    if not raw:
        return None

    raw = raw[:19]

    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass

    return None


def _kpi_event_is_eligible(rt, il = None) -> bool:
    azione = _norm_text(getattr(rt, "Azione", "")).lower()
    topic = _norm_text(getattr(rt, "Topic", "")).lower()
    motivo = _norm_text(getattr(rt, "Motivo", "")).lower()
    payload = _norm_text(getattr(rt, "PayloadJson", "")).lower()

    if azione not in KPI_CLOSURE_ACTIONS:
        return False

    if "eliminato_gestionale" in azione:
        return False

    if "eliminato_gestionale" in topic:
        return False

    if "eliminato dal gestionale" in motivo:
        return False

    if "eliminato_gestionale" in payload:
        return False

    return True


def _kpi_jsonish_list(value) -> list[str]:
    raw = _norm_text(value)
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        return [raw]

    if not isinstance(parsed, list):
        parsed = [parsed]

    return [_norm_text(item) for item in parsed]


def _kpi_active_value_from_list(raw_values, raw_phases, fase: str) -> str:
    values = _kpi_jsonish_list(raw_values)
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


def _kpi_reparto_for_log(rt, il) -> str:
    if il is not None:
        fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(
            getattr(il, "FaseAttiva", "")
        )
        value = _kpi_active_value_from_list(
            getattr(il, "CodReparto", ""),
            getattr(il, "NumFase", ""),
            fase,
        )
        return _first_code_from_cell(value) or _first_code_from_cell(
            getattr(il, "CodReparto", "")
        )

    return _first_code_from_cell(getattr(rt, "CodReparto", ""))


def _kpi_risorsa_for_log(il) -> str:
    if il is None:
        return ""
    fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(getattr(il, "FaseAttiva", ""))
    if fase == _norm_text(getattr(il, "FaseAttiva", "")):
        active = _norm_text(getattr(il, "RisorsaAttiva", ""))
        if active:
            return active
    return _first_code_from_cell(_kpi_active_value_from_list(
        getattr(il, "CodRisorsaProd", ""), getattr(il, "NumFase", ""), fase,
    ))


def _kpi_lavorazione_for_log(il) -> str:
    if il is None:
        return ""
    fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(getattr(il, "FaseAttiva", ""))
    if fase == _norm_text(getattr(il, "FaseAttiva", "")):
        active = _norm_text(getattr(il, "LavorazioneAttiva", ""))
        if active:
            return active
    return _first_code_from_cell(_kpi_active_value_from_list(
        getattr(il, "CodLavorazione", ""), getattr(il, "NumFase", ""), fase,
    ))


def _kpi_parse_datetime(value) -> datetime | None:
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


def _kpi_tempo_previsto_ore(il, *, quantita=None) -> float:
    if il is None:
        return 0.0
    fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(getattr(il, "FaseAttiva", ""))
    minuti_pezzo = max(_safe_float(_kpi_active_value_from_list(
        getattr(il, "TempoPrevistoLavoraz", ""), getattr(il, "NumFase", ""), fase,
    )), 0.0)
    if quantita is None:
        quantita = sum(_kpi_quantita_consuntivata(il))
    return minuti_pezzo * max(quantita, 0.0) / 60.0


def _kpi_tempo_reale_ore(rt, il) -> float:
    for raw in (getattr(il, "TempoFunzionamentoFinale", None),
                getattr(rt, "TempoFunzionamentoPost", None)):
        if raw is None or str(raw).strip() == "":
            continue
        try:
            value = float(str(raw).strip().replace(",", "."))
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value >= 0:
            return value
    elapsed = _safe_float(getattr(rt, "ElapsedSeconds", ""))
    stopped = _safe_float(getattr(rt, "TempoNonFunzionamentoSecondi", ""))
    return max(elapsed - stopped, 0.0) / 3600.0


def _kpi_quantita_consuntivata(
    il,
    rt=None,
) -> tuple[float, float]:
    if il is None:
        return 0.0, 0.0

    # A partial machine closure records missing components, not completed units.
    if _norm_text(getattr(rt, "Azione", "")).lower() == "chiusura_parziale_macchina":
        return 0.0, 0.0
    conforme = max(_safe_float(getattr(il, "QuantitaConforme", "")), 0.0)
    non_conforme = max(
        _safe_float(getattr(il, "QuantitaNonConforme", "")),
        0.0,
    )
    return conforme, non_conforme


def _kpi_closed_at(rt, il) -> datetime | None:
    if il is not None:
        dt = _kpi_parse_datetime(getattr(il, "ClosedAt", ""))
        if dt is not None:
            return dt

    return _kpi_parse_datetime(getattr(rt, "EventAt", ""))


def _kpi_lead_time_ore(
    rt,
    closed_at: datetime,
    started_at: datetime | None = None,
) -> float | None:
    started_at = started_at or _kpi_parse_datetime(
        getattr(rt, "DataInCaricoPre", "")
    ) or _kpi_parse_datetime(getattr(rt, "DataInCaricoPost", ""))

    if started_at is None or closed_at <= started_at:
        return None

    return (closed_at - started_at).total_seconds() / 3600.0


def _kpi_data_fine_prevista(il) -> date | None:
    if il is None:
        return None

    fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(
        getattr(il, "FaseAttiva", "")
    )
    raw = _kpi_active_value_from_list(
        getattr(il, "DataFineSched", ""),
        getattr(il, "NumFase", ""),
        fase,
    )
    return _kpi_parse_date(raw)


def _kpi_is_partial(rt, il=None) -> bool:
    return _norm_text(getattr(rt, "Azione", "")).lower() in {
        "chiusura_parziale", "chiusura_parziale_macchina",
    } or _norm_text(getattr(il, "ChiusuraParziale", "")).lower() in {
        "1", "true", "si", "sì", "yes",
    }


def _kpi_history(rows):
    """Recover phase quantities and first order takeover before applying date filters."""
    starts, quantities, pending = {}, {}, {}
    dated = [
        (rt, il, _kpi_closed_at(rt, il))
        for rt, il in rows if _kpi_event_is_eligible(rt, il)
    ]
    ordered = sorted(
        (row for row in dated if row[2] is not None),
        key=lambda row: (row[2], getattr(row[0], "log_id", 0) or 0),
    )
    for rt, il, closed_at in ordered:
        order_key = (_norm_text(rt.IdDocumento), _norm_text(rt.IdRiga))
        for field in ("DataInCaricoPre", "DataInCaricoPost"):
            started = _kpi_parse_datetime(getattr(rt, field, ""))
            if started is not None and started <= closed_at:
                starts[order_key] = min(starts.get(order_key, started), started)
        fase = _norm_text(getattr(il, "FaseConsuntivata", "")) or _norm_text(getattr(rt, "FasePre", ""))
        phase_key = (*order_key, fase)
        pending[phase_key] = pending.get(phase_key, 0.0) + sum(_kpi_quantita_consuntivata(il, rt))
        if not _kpi_is_partial(rt, il):
            quantities[_norm_text(getattr(rt, "OperationGroupId", "")), *order_key] = pending.pop(phase_key)
    return starts, quantities
