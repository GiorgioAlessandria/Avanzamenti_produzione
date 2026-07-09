from datetime import datetime, date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from zoneinfo import ZoneInfo
import json
from app_odp.ordine_ref import format_ordine_ref_display_from_ordine

ROME_TZ = ZoneInfo("Europe/Rome")


def _norm_text(value) -> str:
    return str(value or "").strip()


def _json_safe(value):
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, set):
        return sorted(_json_safe(v) for v in value)
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _first_not_blank(*values, default=""):
    for value in values:
        text = _norm_text(value)
        if text:
            return text
    return default


def _now_rome_dt() -> datetime:
    return datetime.now(ROME_TZ)


def _parse_iso_dt(value) -> datetime | None:
    raw = _norm_text(value)
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ROME_TZ)
    return dt


def _fase_to_int(value) -> int | None:
    raw = _norm_text(value)
    if not raw:
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _parse_jsonish_list(value) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw = str(value).strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except Exception:
            return [raw]
        raw_items = parsed if isinstance(parsed, list) else [parsed]
    return [s for item in raw_items if (s := _norm_text(item))]


def _parse_phase_list(value) -> list[str]:
    raw = _norm_text(value)
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = None
    if isinstance(parsed, list):
        return [
            str(fase_int)
            for item in parsed
            if (fase_int := _fase_to_int(item)) is not None and fase_int > 0
        ]
    totale_fasi = _fase_to_int(raw)
    return (
        [str(i) for i in range(1, totale_fasi + 1)]
        if totale_fasi and totale_fasi > 0
        else []
    )


def _active_value_for_phase(raw_values, raw_phases, fase_corrente: str) -> str:
    values = _parse_jsonish_list(raw_values)
    phases = _parse_phase_list(raw_phases)
    fase_corrente = _norm_text(fase_corrente)

    if not values:
        return ""

    if phases and len(phases) == len(values):
        for fase, value in zip(phases, values):
            if fase == fase_corrente:
                return _norm_text(value)

    fase_int = _fase_to_int(fase_corrente)
    if fase_int is not None:
        idx = fase_int - 1
        if 0 <= idx < len(values):
            return _norm_text(values[idx])

    return _norm_text(values[0])


def _sync_active_fields_for_phase(ordine, fase_corrente: str | None = None) -> None:
    fase_ref = _norm_text(fase_corrente) or _norm_text(
        getattr(ordine, "FaseAttiva", "")
    )

    ordine.LavorazioneAttiva = _active_value_for_phase(
        getattr(ordine, "CodLavorazione", ""),
        getattr(ordine, "NumFase", ""),
        fase_ref,
    )
    ordine.RisorsaAttiva = _active_value_for_phase(
        getattr(ordine, "CodRisorsaProd", ""),
        getattr(ordine, "NumFase", ""),
        fase_ref,
    )
    ordine.AttrezzaggioAttivo = _active_value_for_phase(
        getattr(ordine, "TempoAttrezzaggio", ""),
        getattr(ordine, "NumFase", ""),
        fase_ref,
    )


def _qty_da_lavorare_text(ordine, stato=None) -> str:
    if stato is not None:
        qty_runtime = _norm_text(getattr(stato, "QtyDaLavorare", ""))
        if qty_runtime:
            return qty_runtime
    return _norm_text(getattr(ordine, "QtyDaLavorare", "")) or _norm_text(
        ordine.Quantita
    )


def _first_code_from_cell(value) -> str:
    for code in _parse_jsonish_list(value):
        if code:
            return code
    return _norm_text(value)


def _tempo_to_seconds(value) -> int:
    raw = _norm_text(value).replace(",", ".")
    if not raw:
        return 0
    try:
        hours = Decimal(raw)
    except InvalidOperation:
        return 0
    return int((hours * Decimal("3600")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _seconds_to_tempo_text(seconds: int) -> str:
    if seconds <= 0:
        return "0"
    hours = (Decimal(seconds) / Decimal("3600")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    text = format(hours, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _parse_qty_decimal(value) -> Decimal:
    raw = _norm_text(value).replace(",", ".")
    if raw == "":
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation:
        raise ValueError(f"Quantità non valida: {value!r}")


def _parse_qty_integer_decimal(value, field_name: str = "Quantità") -> Decimal:
    q = _parse_qty_decimal(value)
    if q != q.to_integral_value():
        raise ValueError(f"{field_name} deve essere un numero intero")
    return q


def _decimal_to_text(value: Decimal) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    s = format(value.normalize(), "f") if value != 0 else "0"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def _safe_float(value) -> float:
    raw = _norm_text(value).replace(",", ".")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _qty_da_lavorare_decimal(ordine, stato=None) -> Decimal:
    return _parse_qty_decimal(_qty_da_lavorare_text(ordine, stato=stato))


def _scaled_component_qty(
    comp_qty,
    q_lavorata: Decimal,
    q_tot: Decimal,
) -> Decimal:
    try:
        base_qty = _parse_qty_decimal(comp_qty)
    except ValueError:
        return Decimal("0")

    if q_tot <= 0:
        return base_qty

    return (base_qty * q_lavorata / q_tot).quantize(
        Decimal("0.0001"),
        rounding=ROUND_HALF_UP,
    )


def _normalize_indice_articolo_search(value) -> str:
    indice = _norm_text(value)
    if not indice:
        return ""

    if indice == "-" or indice.upper() in {"X", "NAN", "NONE", "NULL"}:
        return ""

    return indice


def _normalize_variante_articolo_search(value) -> str:
    variante = _norm_text(value)
    if not variante:
        return ""
    if variante == "-" or variante.upper() == "X":
        return ""
    return variante


def _ordine_state_rank(stato: str) -> int:
    s = _norm_text(stato).lower()
    if "attiv" in s:
        return 0
    if "sospes" in s:
        return 1
    if "pianificat" in s:
        return 2
    return 9


def _parse_distinta_materiale(ordine) -> list[dict]:
    distinta = []
    if ordine.DistintaMateriale:
        try:
            distinta = json.loads(ordine.DistintaMateriale)
            if isinstance(distinta, str):
                distinta = json.loads(distinta)
        except (json.JSONDecodeError, TypeError):
            distinta = []
    return distinta if isinstance(distinta, list) else []


def _ordine_ref_label(ordine) -> str:
    ref = format_ordine_ref_display_from_ordine(ordine)

    if ref:
        return ref

    return f"{_norm_text(ordine.IdDocumento)} {_norm_text(ordine.IdRiga)}".strip()


def _phase_sequence_for_ordine(ordine) -> list[str]:
    fasi = _parse_phase_list(getattr(ordine, "NumFase", ""))
    if fasi:
        return fasi

    fase_corrente = _fase_to_int(getattr(ordine, "FaseAttiva", ""))
    if fase_corrente is not None and fase_corrente > 0:
        return [str(fase_corrente)]

    return []


def _get_phase_transition(ordine, fase_corrente: str) -> tuple[bool, str | None]:
    fasi = _phase_sequence_for_ordine(ordine)
    if not fasi:
        return True, None

    fase_corrente = _norm_text(fase_corrente)
    if fase_corrente not in fasi:
        return True, None

    idx = fasi.index(fase_corrente)
    is_last = idx >= len(fasi) - 1
    next_phase = None if is_last else fasi[idx + 1]
    return is_last, next_phase


def _remaining_phase_codes_for_ordine(ordine) -> set[str]:
    fasi = _phase_sequence_for_ordine(ordine)
    fase_attiva_int = _fase_to_int(getattr(ordine, "FaseAttiva", "")) or 1

    if not fasi:
        return {str(fase_attiva_int)}

    idx = 0
    for i, fase in enumerate(fasi):
        fase_int = _fase_to_int(fase)
        if fase_int is not None and fase_int >= fase_attiva_int:
            idx = i
            break

    out = set()
    for fase in fasi[idx:]:
        fase_int = _fase_to_int(fase)
        if fase_int is not None:
            out.add(str(fase_int))

    return out or {str(fase_attiva_int)}


def _parse_bool_flag(value) -> bool:
    if isinstance(value, bool):
        return value
    raw = _norm_text(value).lower()
    return raw in {"1", "true", "si", "sì", "yes", "on"}


def _row_key(id_documento: str, id_riga: str) -> str:
    return f"{id_documento}|{id_riga}"


def _parse_minuti_non_funzionamento(
    value,
    field_name: str = "Tempo di non funzionamento macchina",
) -> int:
    raw = _norm_text(value)
    if raw == "":
        return 0

    if not raw.isdigit():
        raise ValueError(f"{field_name} deve essere un numero intero >= 0")

    minuti = int(raw)
    if minuti < 0:
        raise ValueError(f"{field_name} deve essere >= 0")

    return minuti


def _ordine_has_distinta_materiale(ordine) -> bool:
    distinta = _parse_distinta_materiale(ordine)
    return any(isinstance(comp, dict) for comp in distinta)


def _resolve_registration_datetime(
    raw_value,
    *,
    allow_override: bool,
    fallback_dt: datetime,
) -> tuple[date, datetime, str]:
    registration_day = fallback_dt.date()

    if allow_override:
        parsed_day = _parse_registration_date_input(raw_value)
        if parsed_day is not None:
            if parsed_day > fallback_dt.date():
                raise ValueError("La data registrazione non può essere futura.")
            registration_day = parsed_day

    registration_dt = datetime.combine(
        registration_day,
        fallback_dt.timetz().replace(microsecond=0),
    )
    registration_date_text = registration_day.strftime("%d/%m/%Y")
    return registration_day, registration_dt, registration_date_text


def _parse_registration_date_input(value) -> date | None:
    raw = _norm_text(value)
    if not raw:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue

    raise ValueError("Data registrazione non valida.")


def _extract_codes_from_cell(value) -> list[str]:
    """
    Normalizza celle che possono contenere:
    - "10"
    - ["10"]
    - [["10"]]
    - {"key": "10"}
    """
    if value in (None, ""):
        return []

    def walk(node):
        if isinstance(node, dict):
            for v in node.values():
                yield from walk(v)
        elif isinstance(node, (list, tuple, set)):
            for item in node:
                yield from walk(item)
        else:
            s = str(node).strip()
            if s:
                yield s

    if isinstance(value, (dict, list, tuple, set)):
        return list(dict.fromkeys(walk(value)))

    raw = str(value).strip()
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        return [raw]

    return list(dict.fromkeys(walk(parsed)))


def _bool_text(value: bool) -> str:
    return "true" if bool(value) else "false"
