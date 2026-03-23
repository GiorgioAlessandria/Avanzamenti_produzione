from datetime import datetime
from zoneinfo import ZoneInfo
import json
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from sqlalchemy.orm import selectinload
from flask import (
    Blueprint,
    render_template,
    request,
    url_for,
    abort,
    jsonify,
    current_app,
)
from flask_login import login_required, current_user
from sqlalchemy import func, select
from app_odp.etichette import gen_etichette
from app_odp.models import (
    InputOdp,
    InputOdpRuntime,
    db,
    ChangeEvent,
    Causaliattivita,
    GiacenzaLotti,
    LottiUsatiLog,
    ErpOutbox,
    InputOdpLog,
    StatoOdpLog,
    ChangeEventLog,
    LottiGeneratiLog,
)
from app_odp.policy.decorator import require_perm
from app_odp.policy.policy import RbacPolicy

try:
    from icecream import ic
finally:
    pass

main_bp = Blueprint("main", __name__)
ROME_TZ = ZoneInfo("Europe/Rome")

# region FUNZIONI

HOME_TABS = {
    "10": {
        "tab": "montaggio",
        "label_fallback": "Montaggio",
        "template": "partials/_home_montaggio.j2",
    },
    "20": {
        "tab": "officina",
        "label_fallback": "Officina",
        "template": "partials/_home_standard.j2",
    },
    "30": {
        "tab": "carpenteria",
        "label_fallback": "Carpenteria",
        "template": "partials/_home_standard.j2",
    },
    "40": {
        "tab": "Magazzino",
        "label_fallback": "Magazzino",
        "template": "partials/page_vuota.html",
    },
    "50": {
        "tab": "Fornitori",
        "label_fallback": "Fornitori",
        "template": "partials/page_vuota.html",
    },
    "60": {
        "tab": "Ufficio Tecnico",
        "label_fallback": "Ufficio Tecnico",
        "template": "partials/page_vuota.html",
    },
    "70": {
        "tab": "collaudo",
        "label_fallback": "Collaudo",
        "template": "partials/_home_montaggio.j2",
    },
}

TAB_TO_TEMPLATE = {
    "montaggio": ("partials/_home_montaggio.j2", {"reparto": "10", "perm": "home"}),
    "officina": ("partials/_home_standard.j2", {"reparto": "20", "perm": "home"}),
    "carpenteria": (
        "partials/_home_standard.j2",
        {"reparto": "30", "perm": "home"},
    ),
    "collaudo": (
        "partials/_home_montaggio.j2",
        {"reparto": "70", "perm": "home"},
    ),
}
BRIDGE_CONFIG = {
    "officina": {"reparto": "20", "perm": "home", "renderer": "officina"},
    "carpenteria": {"reparto": "30", "perm": "home", "renderer": "carpenteria"},
    "montaggio": {"reparto": "10", "perm": "home", "renderer": "montaggio"},
    "collaudo": {"reparto": "70", "perm": "home", "renderer": "collaudo"},
}


def _tab_scoped_odp(policy: RbacPolicy, reparto_code: str):
    q = _base_odp_query()
    return policy.filter_input_odp_for_reparto(q, reparto_code)


def _base_odp_query():
    return InputOdp.query.options(
        selectinload(InputOdp.runtime_row),
    )


def _last_change_event_id() -> int:
    return db.session.query(func.max(ChangeEvent.id)).scalar() or 0


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


def _parse_bool_flag(value) -> bool:
    if isinstance(value, bool):
        return value
    raw = _norm_text(value).lower()
    return raw in {"1", "true", "si", "sì", "yes", "on"}


def _decimal_to_text(value: Decimal) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    s = format(value.normalize(), "f") if value != 0 else "0"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def _qty_da_lavorare_text(ordine) -> str:
    return _norm_text(getattr(ordine, "QtyDaLavorare", "")) or _norm_text(
        ordine.Quantita
    )


def _qty_da_lavorare_decimal(ordine) -> Decimal:
    return _parse_qty_decimal(_qty_da_lavorare_text(ordine))


def _get_blocking_outbox_for_phase(
    id_documento: str,
    id_riga: str,
    fase: str,
):
    fase = _norm_text(fase)
    if not fase:
        return None

    return (
        ErpOutbox.query.filter_by(
            IdDocumento=id_documento,
            IdRiga=id_riga,
            Fase=fase,
        )
        .filter(ErpOutbox.status.in_(["pending", "error"]))
        .order_by(ErpOutbox.outbox_id.desc())
        .first()
    )


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


def _fase_attiva_int(ordine) -> int | None:
    try:
        return int(float(_norm_text(ordine.FaseAttiva)))
    except (ValueError, TypeError):
        return None


def _fase_to_int(value) -> int | None:
    s = _norm_text(value)
    if not s:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def _parse_phase_list(value) -> list[str]:
    raw = _norm_text(value)
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = None

    if isinstance(parsed, list):
        out = []
        for item in parsed:
            fase_int = _fase_to_int(item)
            if fase_int is not None and fase_int > 0:
                out.append(str(fase_int))
        return out

    totale_fasi = _fase_to_int(raw)
    if totale_fasi is not None and totale_fasi > 0:
        return [str(i) for i in range(1, totale_fasi + 1)]

    return []


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


def _set_runtime_pianificata(stato, username: str):
    if stato is None:
        return
    stato.Stato_odp = "Pianificata"
    stato.Utente_operazione = username
    stato.data_ultima_attivazione = None


def _set_runtime_sospeso(stato, username: str, fase_corrente: str):
    if stato is None:
        return
    stato.Stato_odp = "In Sospeso"
    stato.Utente_operazione = username
    if fase_corrente:
        stato.FaseAttiva = fase_corrente
    stato.data_ultima_attivazione = None


def _advance_or_finalize_phase(
    *,
    ordine,
    stato,
    fase_corrente: str,
    q_ok: Decimal,
    q_nok: Decimal,
    qty_residua: Decimal,
    qty_residua_text: str,
    qty_lavorata_text: str,
    chiusura_parziale: bool,
    username: str,
):
    is_last_phase, next_phase = _get_phase_transition(ordine, fase_corrente)

    if chiusura_parziale:
        ordine.FaseAttiva = fase_corrente
        ordine.StatoOrdine = "In Sospeso"
        ordine.QtyDaLavorare = qty_residua_text
        _sync_active_fields_for_phase(ordine, fase_corrente)
        _set_runtime_sospeso(stato, username, fase_corrente)
        return {
            "tipo": "parziale_stessa_fase",
            "fase_corrente": fase_corrente,
            "fase_successiva": fase_corrente,
        }

    if is_last_phase:
        ordine.FaseAttiva = fase_corrente
        ordine.StatoOrdine = "Chiusa"
        ordine.QtyDaLavorare = "0"
        _sync_active_fields_for_phase(ordine, fase_corrente)
        return {
            "tipo": "finale",
            "fase_corrente": fase_corrente,
            "fase_successiva": None,
        }

    ordine.FaseAttiva = next_phase
    ordine.StatoOrdine = "Pianificata"
    ordine.QtyDaLavorare = _decimal_to_text(q_ok)
    _sync_active_fields_for_phase(ordine, next_phase)
    _set_runtime_pianificata(stato, username)

    return {
        "tipo": "avanzata",
        "fase_corrente": fase_corrente,
        "fase_successiva": next_phase,
    }


def _fase_corrente_for_export(ordine, stato=None, fase_override="") -> str:
    raw = (
        _norm_text(fase_override)
        or _norm_text(getattr(stato, "FaseAttiva", ""))
        or _norm_text(getattr(ordine, "FaseAttiva", ""))
    )
    fase_int = _fase_to_int(raw)
    if fase_int is not None and fase_int > 0:
        return str(fase_int)

    fasi = _phase_sequence_for_ordine(ordine)
    if len(fasi) == 1:
        return fasi[0]

    return ""


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

    out = []
    for item in raw_items:
        s = _norm_text(item)
        if s:
            out.append(s)
    return out


def _active_value_for_phase(raw_values, raw_phases, fase_corrente: str) -> str:
    values = _parse_jsonish_list(raw_values)
    phases = _parse_phase_list(raw_phases)
    fase_corrente = _norm_text(fase_corrente)

    if not values:
        return ""

    # caso migliore: liste allineate per fase
    if phases and len(phases) == len(values):
        for fase, value in zip(phases, values):
            if fase == fase_corrente:
                return _norm_text(value)

    # fallback per indice fase (1-based)
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


def _componenti_lotto_per_ordine(
    ordine,
    include_senza_lotti: bool = False,
    ignore_parent_gestione_lotto: bool = False,
    **_unused,
) -> list[dict]:
    if not ignore_parent_gestione_lotto:
        if _norm_text(ordine.GestioneLotto).lower() != "si":
            return []

    distinta = _parse_distinta_materiale(ordine)
    fase_attiva = _fase_attiva_int(ordine)

    componenti_lotto = []
    codici_visti = set()

    for comp in distinta:
        if not isinstance(comp, dict):
            continue

        if fase_attiva is not None:
            try:
                comp_fase = int(float(comp.get("NumFase", 0)))
            except (ValueError, TypeError):
                comp_fase = 0
            if comp_fase != fase_attiva:
                continue

        comp_gl = _norm_text(comp.get("GestioneLotto", "")).lower()
        if comp_gl != "si":
            continue

        cod_art = _norm_text(comp.get("CodArt", ""))
        if not cod_art or cod_art in codici_visti:
            continue

        codici_visti.add(cod_art)

        lotti_db = GiacenzaLotti.query.filter_by(CodArt=cod_art).all()
        lotti_list = []
        for lotto in lotti_db:
            try:
                giacenza_val = int(float(_norm_text(lotto.Giacenza)))
            except (ValueError, TypeError):
                giacenza_val = 0

            if giacenza_val <= 0:
                continue

            lotti_list.append(
                {
                    "RifLottoAlfa": lotto.RifLottoAlfa,
                    "Giacenza": giacenza_val,
                    "CodMag": lotto.CodMag,
                }
            )

        if include_senza_lotti or lotti_list:
            componenti_lotto.append(
                {
                    "CodArt": cod_art,
                    "DesArt": _norm_text(comp.get("DesArt", "")),
                    "Quantita": comp.get("Quantita", 0),
                    "NumFase": comp.get("NumFase", ""),
                    "GestioneLotto": "si",
                    "lotti": lotti_list,
                }
            )

    return componenti_lotto


def _same_decimal_qty(a: Decimal, b: Decimal, tol: Decimal = Decimal("0.0001")) -> bool:
    return abs(a - b) <= tol


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


def _normalize_lotti_for_payload(lotti_input: list[dict]) -> list[dict]:
    rows = []
    for row in lotti_input or []:
        rows.append(
            {
                "CodArt": _norm_text(row.get("CodArt")),
                "RifLottoAlfa": _norm_text(row.get("RifLottoAlfa")),
                "Quantita": str(row.get("Quantita", 0)),
                "Esito": _norm_text(row.get("Esito", "ok")),
            }
        )
    return rows


def _normalize_lotto_prodotto_for_payload(lotto: dict | None) -> dict | None:
    if not lotto:
        return None

    parent_lotti = []
    for row in lotto.get("ParentLotti") or []:
        parent_lotti.append(
            {
                "cod_art": _norm_text(row.get("CodArt")),
                "rif_lotto_alfa": _norm_text(row.get("RifLottoAlfa")),
                "quantita": _norm_text(row.get("Quantita")),
            }
        )

    return {
        "cod_art": _norm_text(lotto.get("CodArt")),
        "rif_lotto_alfa": _norm_text(lotto.get("RifLottoAlfa")),
        "quantita": _norm_text(lotto.get("Quantita")),
        "fase": _norm_text(lotto.get("Fase")),
        "parent_lotti": parent_lotti,
    }


def _current_username(default: str = "utente_sconosciuto") -> str:
    return (
        getattr(current_user, "username", None)
        or getattr(current_user, "name", None)
        or getattr(current_user, "email", None)
        or str(getattr(current_user, "id", default))
    )


def _build_phase_payload(
    ordine,
    fase_corrente: str,
    q_ok: Decimal,
    q_nok: Decimal,
    tempo_finale: str,
    lotti_input: list[dict],
    lotto_prodotto: dict | None,
    note: str,
    now_iso: str,
    chiusura_parziale: bool = False,
) -> dict:
    return {
        "kind": "consuntivo_fase",
        "id_documento": ordine.IdDocumento,
        "id_riga": ordine.IdRiga,
        "rif_registraz": ordine.RifRegistraz,
        "cod_art": ordine.CodArt,
        "descrizione": ordine.DesArt,
        "fase": fase_corrente,
        "cod_reparto": ordine.CodReparto,
        "quantita_ordine": _norm_text(ordine.Quantita),
        "quantita_da_lavorare": _qty_da_lavorare_text(ordine),
        "quantita_ok": str(q_ok),
        "quantita_ko": str(q_nok),
        "tempo_funzionamento": tempo_finale,
        "note": note,
        "lotti": _normalize_lotti_for_payload(lotti_input),
        "lotto_prodotto": _normalize_lotto_prodotto_for_payload(lotto_prodotto),
        "created_at": now_iso,
        "created_by": _current_username(),
        "chiusura_parziale": chiusura_parziale,
    }


def _queue_phase_export(ordine, fase_corrente: str, payload: dict):
    outbox = ErpOutbox(
        kind="consuntivo_fase",
        status="pending",
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        CodArt=ordine.CodArt,
        Fase=fase_corrente,
        CodReparto=_norm_text(ordine.CodReparto),
        payload_json=json.dumps(payload, ensure_ascii=False),
    )
    db.session.add(outbox)
    db.session.flush()
    return outbox


def _safe_txt_suffix(value: str, fallback: str = "export") -> str:
    raw = _norm_text(value)
    if not raw:
        return fallback

    out = []
    for ch in raw:
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        else:
            out.append("_")

    cleaned = "".join(out).strip("_")
    return cleaned or fallback


def _get_erp_export_dir() -> Path:
    """
    Recupera la cartella export dai config caricati nell'app factory.
    Se manca, usa una cartella locale di fallback.
    """
    raw = current_app.config.get("ERP_EXPORT_DIR", "")
    if raw:
        export_dir = Path(raw)
    else:
        export_dir = Path(current_app.instance_path) / "erp_exports"

    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir


def _build_export_txt_path(prefix: str = "AVPB", suffix: str = "") -> Path:
    now_txt = _now_rome_dt().strftime("%Y%m%d_%H%M%S")
    safe_suffix = _safe_txt_suffix(suffix, "export")
    file_name = f"{prefix}_{safe_suffix}_{now_txt}.txt"
    return _get_erp_export_dir() / file_name


def _write_txt_content(
    content: str,
    *,
    prefix: str = "AVPB",
    suffix: str = "",
    encoding: str = "utf-8-sig",
) -> Path:
    path_txt = _build_export_txt_path(prefix=prefix, suffix=suffix)
    newline_character = "\n"
    path_txt.write_text(content, encoding=encoding, newline=newline_character)
    return path_txt


AVP_COLUMNS = [
    "TipoRecord",  # 1
    "TESTipoDoc",  # 10
    "TESDataReg",  # 20
    "TESNReg",  # 30
    "TESApp",  # 40
    "RIGTipoOpAvp",  # 80
    "RIGRifORP",  # 90
    "RIGCodArt",  # 100
    "RIGQta",  # 140
    "RIGMagPrinc",  # 210
    "RIGCodRisorsa",  # 300
    "RIGCausalePrest",  # 310
    "RIGOreLav",  # 322
]

AVP_DEFAULTS = {
    # TES
    "tes_tipo_documento": 704,
    "tes_numero_registrazione": 0,
    "tes_appendice": "",
    # RIG
    "rig_tipo_op_qta": 702,
    "rig_tipo_op_ore": 709,
    "rig_magazzino_principale": "0",
    "rig_causale_prestazione": 0,
    # campo 90
    # possibili valori:
    # - "raw_rif_registraz"
    # - "riga"
    # - "riga_fase"
    # - "barcode17"
    # - "barcode22"
    "rif90_mode": "raw_rif_registraz",
    # quantità esportata:
    # - "ok"      => solo quantita_ok
    # - "worked"  => quantita_ok + quantita_ko
    "qta_mode": "ok",
    # il file esempio che hai incollato NON è dello stesso tracciato,
    # quindi l'header conviene lasciarlo disattivato di default
    "include_header": False,
}


def _erp_avp_cfg() -> dict:
    cfg = dict(AVP_DEFAULTS)
    cfg.update(current_app.config.get("ERP_AVP_DEFAULTS", {}) or {})
    return cfg


def _json_loads_safe(raw, default):
    try:
        return json.loads(raw)
    except Exception:
        return default


def _get_pending_avp_outbox() -> list[ErpOutbox]:
    return (
        ErpOutbox.query.filter(
            ErpOutbox.kind == "consuntivo_fase",
            ErpOutbox.status == "pending",
        )
        .order_by(ErpOutbox.outbox_id.asc())
        .all()
    )


def _get_outbox_payload(outbox: ErpOutbox) -> dict:
    payload = _json_loads_safe(outbox.payload_json or "{}", {})
    return payload if isinstance(payload, dict) else {}


def _get_export_source_row(outbox: ErpOutbox):
    """
    Prova prima su InputOdp corrente.
    Se non esiste più, ripiega sull'ultimo snapshot InputOdpLog.
    """
    ordine = InputOdp.query.filter_by(
        IdDocumento=outbox.IdDocumento,
        IdRiga=outbox.IdRiga,
    ).first()
    if ordine is not None:
        return ordine

    return (
        InputOdpLog.query.filter_by(
            IdDocumento=outbox.IdDocumento,
            IdRiga=outbox.IdRiga,
        )
        .order_by(InputOdpLog.log_id.desc())
        .first()
    )


def _first_not_blank(*values, default=""):
    for value in values:
        text = _norm_text(value)
        if text:
            return text
    return default


def _format_datetime_for_avp(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return _now_rome_dt().strftime("%d/%m/%Y %H:%M:%S")

    dt = _parse_iso_dt(raw)
    if dt is not None:
        return dt.strftime("%d/%m/%Y %H:%M:%S")

    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=ROME_TZ)
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except ValueError:
            pass

    return raw


def _format_decimal_it(value, places: int = 2) -> str:
    try:
        dec = _parse_qty_decimal(value)
    except ValueError:
        dec = Decimal("0")

    quant = Decimal("1") if places <= 0 else Decimal("1." + ("0" * places))
    dec = dec.quantize(quant, rounding=ROUND_HALF_UP)
    return f"{dec:.{places}f}".replace(".", ",")


def _zero_fill_digits(value, width: int) -> str:
    raw = re.sub(r"\D+", "", _norm_text(value))
    if not raw:
        raw = "0"
    return raw.zfill(width)


def _build_rif_orp(payload: dict, cfg: dict) -> str:
    mode = _norm_text(cfg.get("rif90_mode")) or "raw_rif_registraz"

    rif_reg = _norm_text(payload.get("rif_registraz"))
    id_doc = _norm_text(payload.get("id_documento"))
    id_riga = _norm_text(payload.get("id_riga"))
    fase = _norm_text(payload.get("fase"))

    if mode == "raw_rif_registraz":
        return rif_reg

    if mode == "riga":
        if rif_reg:
            return ".".join([x for x in [rif_reg, id_riga] if x])
        return ""

    if mode == "riga_fase":
        if rif_reg:
            return ".".join([x for x in [rif_reg, id_riga, fase] if x])
        return ""

    if mode == "barcode17":
        return (
            f"{_zero_fill_digits(id_doc, 9)}"
            f"{_zero_fill_digits(id_riga, 4)}"
            f"{_zero_fill_digits(fase, 4)}"
        )

    if mode == "barcode22":
        return (
            f"{_zero_fill_digits(id_doc, 9)}"
            f"{_zero_fill_digits(id_riga, 9)}"
            f"{_zero_fill_digits(fase, 4)}"
        )

    return rif_reg


def _pick_resource_code(source_row, fase_corrente: str) -> str:
    if source_row is None:
        return ""

    raw_risorse = _norm_text(getattr(source_row, "CodRisorsaProd", ""))
    raw_fasi = _norm_text(getattr(source_row, "NumFase", ""))

    # Per l'export conta la risorsa della fase esportata,
    # non la RisorsaAttiva corrente della riga.
    by_phase = _active_value_for_phase(raw_risorse, raw_fasi, fase_corrente)
    if by_phase:
        return by_phase

    risorsa_attiva = _norm_text(getattr(source_row, "RisorsaAttiva", ""))
    if risorsa_attiva:
        return risorsa_attiva

    return raw_risorse


def _pick_magazzino_principale(source_row, cfg: dict) -> str:
    if source_row is None:
        return _norm_text(cfg.get("rig_magazzino_principale", "0"))
    return _first_not_blank(
        getattr(source_row, "CodMagPrincipale", ""),
        cfg.get("rig_magazzino_principale", "0"),
    )


def _pick_tipo_documento(source_row, cfg: dict):
    tipo_doc = _norm_text(getattr(source_row, "CodTipoDoc", "")) if source_row else ""
    return tipo_doc or cfg.get("tes_tipo_documento", 704)


def _pick_qta_export(payload: dict, cfg: dict) -> Decimal:
    try:
        q_ok = _parse_qty_decimal(payload.get("quantita_ok"))
    except ValueError:
        q_ok = Decimal("0")

    try:
        q_ko = _parse_qty_decimal(payload.get("quantita_ko"))
    except ValueError:
        q_ko = Decimal("0")

    mode = _norm_text(cfg.get("qta_mode")) or "ok"
    if mode == "worked":
        return q_ok + q_ko

    return q_ok


def _serialize_avp_cell(value, numeric: bool = False) -> str:
    if value is None:
        value = ""

    text = str(value)

    if numeric:
        return text

    escaped = text.replace('"', '""')
    return f'"{escaped}"'


def _serialize_avp_row(values: list, numeric_indexes: set[int] | None = None) -> str:
    numeric_indexes = numeric_indexes or set()
    rendered = []
    for idx, value in enumerate(values):
        rendered.append(_serialize_avp_cell(value, numeric=(idx in numeric_indexes)))
    return ";".join(rendered)


def _build_tes_row(first_payload: dict, source_row, cfg: dict) -> list:
    tipo_doc = _pick_tipo_documento(source_row, cfg)
    data_reg = _format_datetime_for_avp(first_payload.get("created_at"))
    n_reg = cfg.get("tes_numero_registrazione", 0)
    tes_app = cfg.get("tes_appendice", "")

    return [
        "TES",  # 1
        tipo_doc,  # 10
        data_reg,  # 20
        n_reg,  # 30
        tes_app,  # 40
        0,  # 80
        "",  # 90
        "",  # 100
        _format_decimal_it(0, 2),  # 140
        "",  # 210
        "",  # 300
        0,  # 310
        _format_decimal_it(0, 3),  # 322
    ]


def _build_rig_row(payload: dict, source_row, cfg: dict) -> list | None:
    qta = _pick_qta_export(payload, cfg)

    try:
        ore = _parse_qty_decimal(payload.get("tempo_funzionamento"))
    except ValueError:
        ore = Decimal("0")

    # se entrambe zero/non valorizzate, non esportare nulla
    if qta <= 0 and ore <= 0:
        return None

    tipo_doc = _pick_tipo_documento(source_row, cfg)
    data_reg = _format_datetime_for_avp(payload.get("created_at"))
    n_reg = cfg.get("tes_numero_registrazione", 0)
    tes_app = cfg.get("tes_appendice", "")

    fase = _norm_text(payload.get("fase"))

    return [
        "RIG",  # 1
        tipo_doc,  # 10
        data_reg,  # 20
        n_reg,  # 30
        tes_app,  # 40
        cfg.get("rig_tipo_op_qta", 702),  # 80
        _build_rif_orp(payload, cfg),  # 90
        _norm_text(payload.get("cod_art")),  # 100
        _format_decimal_it(qta, 2),  # 140
        _pick_magazzino_principale(source_row, cfg),  # 210
        _pick_resource_code(source_row, fase),  # 300
        cfg.get("rig_causale_prestazione", 0),  # 310
        _format_decimal_it(ore, 3),  # 322
    ]


def _build_avp_txt_content(outbox_rows: list[ErpOutbox]) -> str:
    if not outbox_rows:
        raise ValueError("Nessun record pending da esportare")

    cfg = _erp_avp_cfg()
    lines = []

    if cfg.get("include_header"):
        lines.append(_serialize_avp_row(AVP_COLUMNS))

    for outbox in outbox_rows:
        payload = _get_outbox_payload(outbox)
        source_row = _get_export_source_row(outbox)

        rig_row = _build_rig_row(payload, source_row, cfg)
        if rig_row is not None:
            lines.append(
                _serialize_avp_row(
                    rig_row,
                    numeric_indexes={1, 3, 5, 8, 11, 12},
                )
            )

    return "\n".join(lines) + "\n"


@main_bp.context_processor
def inject_policy_and_nav():
    if not current_user.is_authenticated:
        return {}

    policy = RbacPolicy(current_user)
    items = []

    # voci reparto da DB + policy
    for cod, descr in policy.allowed_reparti_menu:
        cfg = HOME_TABS.get(str(cod))
        if not cfg:
            continue
        items.append(
            {
                "label": descr or cfg["label_fallback"],
                "url": url_for(".home", tab=cfg["tab"]),
                "tab": cfg["tab"],
            }
        )
    return {"policy": policy, "home_switch_items": items}


# region PERCORSI
@main_bp.route("/")
@login_required
@require_perm("home")
def home():
    policy = RbacPolicy(current_user)
    tab = request.args.get("tab")

    # default: prima tab consentita
    if not tab:
        for t, (_, req) in TAB_TO_TEMPLATE.items():
            if req.get("reparto") in policy.allowed_reparti and policy.can(req["perm"]):
                tab = t
                break

    cfg = TAB_TO_TEMPLATE.get(tab)
    if not cfg:
        abort(404)

    template, req = cfg
    if req.get("reparto") not in policy.allowed_reparti:
        abort(403)
    if not policy.can(req["perm"]):
        abort(403)

    q = _tab_scoped_odp(policy, req["reparto"])
    odp = list(q.all())

    causali = (
        db.session.execute(
            select(Causaliattivita.DesCausaleAttivita).order_by(
                Causaliattivita.DesCausaleAttivita
            )
        )
        .scalars()
        .all()
    )
    ic(odp)
    return render_template(
        "home.j2",
        active_partial=template,
        active_tab=tab,
        policy=policy,
        odp=odp,
        causali_attivita=causali,
        bridge_url=url_for("main.api_home_bridge", tab=tab),
        bridge_last_event_id=_last_change_event_id(),
    )


def _query_for_tab(policy, reparto_code):
    q = _base_odp_query()
    q = policy.filter_input_odp_for_reparto(q, reparto_code)
    return q


def _render_bridge_standard(odp):
    return {
        "tbody_ordini_da_eseguire": render_template(
            "partials/_home_standard_rows_da_eseguire.j2", odp=odp
        ),
        "tbody_ordini_in_corso": render_template(
            "partials/_home_standard_rows_in_corso.j2", odp=odp
        ),
    }


def _render_bridge_carpenteria(odp):
    return {
        "tbody_ordini_da_eseguire": render_template(
            "partials/_home_standard_rows_da_eseguire.j2", odp=odp
        ),
        "tbody_ordini_in_corso": render_template(
            "partials/_home_standard_rows_in_corso.j2", odp=odp
        ),
    }


def _render_bridge_montaggio(odp):
    return {
        "tbody_ordini_da_eseguire_sl": render_template(
            "partials/_home_montaggio_sl_rows_da_eseguire.j2", odp=odp
        ),
        "tbody_ordini_in_corso_sl": render_template(
            "partials/_home_montaggio_sl_rows_in_corso.j2", odp=odp
        ),
        "tbody_ordini_da_eseguire_m": render_template(
            "partials/_home_montaggio_m_rows_da_eseguire.j2", odp=odp
        ),
        "tbody_ordini_in_corso_m": render_template(
            "partials/_home_montaggio_m_rows_in_corso.j2", odp=odp
        ),
    }


def _render_bridge_collaudo(odp):
    return {
        "tbody_tbl_da_eseguire_sl": render_template(
            "partials/_home_montaggio_sl_rows_da_eseguire.j2", odp=odp
        ),
        "tbody_ordini_in_corso_sl": render_template(
            "partials/_home_montaggio_sl_rows_in_corso.j2", odp=odp
        ),
        "tbody_tbl_da_eseguire_m": render_template(
            "partials/_home_montaggio_m_rows_da_eseguire.j2", odp=odp
        ),
        "tbody_ordini_in_corso_m": render_template(
            "partials/_home_montaggio_m_rows_in_corso.j2", odp=odp
        ),
    }


RENDERERS = {
    "officina": _render_bridge_standard,
    "carpenteria": _render_bridge_standard,
    "montaggio": _render_bridge_montaggio,
    "collaudo": _render_bridge_collaudo,
}


@main_bp.get("/api/home/<tab>/bridge")
@login_required
@require_perm("home")
def api_home_bridge(tab):
    cfg = BRIDGE_CONFIG.get(tab)
    if not cfg:
        abort(404)

    policy = RbacPolicy(current_user)

    if cfg["reparto"] not in policy.allowed_reparti:
        abort(403)
    if not policy.can(cfg["perm"]):
        abort(403)

    after = request.args.get("after", type=int, default=0)
    last_event_id = _last_change_event_id()

    if after and last_event_id <= after:
        return {"changed": False, "last_event_id": last_event_id}

    odp = list(_query_for_tab(policy, cfg["reparto"]).all())
    fragments = RENDERERS[tab](odp)
    return {
        "changed": True,
        "last_event_id": last_event_id,
        "fragments": fragments,
    }


def _row_key(id_documento: str, id_riga: str) -> str:
    return f"{id_documento}|{id_riga}"


def _norm_text(value) -> str:
    return str(value or "").strip()


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


def _ensure_stato_attivo(
    ordine,
    stato,
    username: str,
    when_dt: datetime,
    fase_corrente: str,
):
    now_iso = when_dt.isoformat(timespec="seconds")

    if stato is None:
        stato = InputOdpRuntime(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            Stato_odp="Attivo",
            Data_in_carico=now_iso,
            Tempo_funzionamento="0",
            Utente_operazione=username,
            FaseAttiva=fase_corrente,
            data_ultima_attivazione=now_iso,
            Note=_norm_text(getattr(ordine, "Note", "")),
            QtyDaLavorare=_qty_da_lavorare_text(ordine),
            RisorsaAttiva=_norm_text(getattr(ordine, "RisorsaAttiva", "")),
            LavorazioneAttiva=_norm_text(getattr(ordine, "LavorazioneAttiva", "")),
        )
        db.session.add(stato)
        return stato

    stato.Stato_odp = "Attivo"
    stato.Utente_operazione = username
    if fase_corrente:
        stato.FaseAttiva = fase_corrente
    if not _norm_text(stato.Data_in_carico):
        stato.Data_in_carico = now_iso
    if not _norm_text(stato.Tempo_funzionamento):
        stato.Tempo_funzionamento = "0"

    stato.data_ultima_attivazione = now_iso
    return stato


def _accumulate_runtime_until(stato, end_dt: datetime) -> int:
    if stato is None:
        return 0

    start_dt = _parse_iso_dt(stato.data_ultima_attivazione)
    if start_dt is None:
        stato.data_ultima_attivazione = None
        if not _norm_text(stato.Tempo_funzionamento):
            stato.Tempo_funzionamento = "0"
        return 0

    elapsed_seconds = max(0, int((end_dt - start_dt).total_seconds()))
    total_seconds = _tempo_to_seconds(stato.Tempo_funzionamento) + elapsed_seconds

    stato.Tempo_funzionamento = _seconds_to_tempo_text(total_seconds)
    stato.data_ultima_attivazione = None
    return elapsed_seconds


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


def _get_visible_odp_by_key(
    policy: RbacPolicy, id_documento: str, id_riga: str
) -> InputOdp:
    ordine = (
        policy.filter_input_odp(_base_odp_query())
        .filter_by(IdDocumento=id_documento, IdRiga=id_riga)
        .first()
    )
    if ordine:
        return ordine

    exists_anyway = (
        _base_odp_query()
        .filter_by(
            IdDocumento=id_documento,
            IdRiga=id_riga,
        )
        .first()
    )

    if exists_anyway is None:
        abort(404)

    abort(403)


def _tab_from_ordine(ordine: InputOdp) -> str | None:
    reparto_codes = set(_extract_codes_from_cell(ordine.CodReparto))
    for tab, cfg in BRIDGE_CONFIG.items():
        if cfg["reparto"] in reparto_codes:
            return tab
    return None


def _fragments_for_ordine_tab(
    policy: RbacPolicy, ordine: InputOdp
) -> tuple[str | None, dict]:
    tab = _tab_from_ordine(ordine)
    if not tab:
        return None, {}

    reparto_code = BRIDGE_CONFIG[tab]["reparto"]
    odp = list(_query_for_tab(policy, reparto_code).all())
    fragments = RENDERERS[tab](odp)
    return tab, fragments


def _push_change_event(
    topic: str, ordine: InputOdp, extra_payload: dict | None = None
) -> ChangeEvent:
    reparto_codes = _extract_codes_from_cell(ordine.CodReparto)
    payload = {
        "id_documento": ordine.IdDocumento,
        "id_riga": ordine.IdRiga,
        "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
        "rif_registraz": ordine.RifRegistraz,
        "cod_reparto": ordine.CodReparto,
        "stato_ordine": ordine.StatoOrdine,
    }
    if extra_payload:
        payload.update(extra_payload)

    evt = ChangeEvent(
        topic=topic,
        scope=reparto_codes[0] if reparto_codes else str(ordine.CodReparto or ""),
        payload_json=json.dumps(payload),
    )
    db.session.add(evt)
    return evt


def _delete_closed_order_from_runtime_db(ordine, stato=None) -> None:
    """
    Elimina l'ordine dal DB runtime principale dopo aver già salvato tutto nel db_log.
    Cancella sia InputOdpRuntime sia InputOdp.
    """
    if stato is None:
        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

    if stato is not None:
        db.session.delete(stato)

    db.session.delete(ordine)
    db.session.flush()


@main_bp.post("/api/ordini/presa")
@login_required
@require_perm("home")
def api_prendi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    fase_corrente = _fase_corrente_for_export(ordine)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Presa in carico bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_change_event_id(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "pianificata":
        now_dt = _now_rome_dt()

        ordine.StatoOrdine = "Attivo"
        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        _push_change_event(
            topic="ordine_preso",
            ordine=ordine,
            extra_payload={
                "azione": "presa_in_carico",
                "utente": _current_username(),
                "fase": fase_corrente,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
                "tempo_funzionamento": stato.Tempo_funzionamento,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine preso in carico"

    elif stato_norm == "attivo":
        message = "Ordine già attivo"

    elif stato_norm == "in sospeso":
        message = "Ordine in sospeso: usare la riattivazione"

    else:
        message = f"Ordine non prendibile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "fase": fase_corrente,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/sospendi")
@login_required
@require_perm("home")
def api_sospendi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    causale = _norm_text(data.get("causale"))

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()

    changed = False
    message = None
    elapsed_seconds = 0
    tempo_funzionamento = "0"

    if stato_norm == "attivo":
        now_dt = _now_rome_dt()
        ordine.StatoOrdine = "In Sospeso"

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        # Se manca il record di runtime,
        # la sospensione non può calcolare correttamente il tempo.
        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine. "
                            "La sospensione non può aggiornare Tempo_funzionamento."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        stato.Stato_odp = "In Sospeso"
        stato.Utente_operazione = _current_username()

        elapsed_seconds = _accumulate_runtime_until(stato, now_dt)
        stato.Stato_odp = "In Sospeso"
        tempo_funzionamento = _norm_text(stato.Tempo_funzionamento) or "0"

        _push_change_event(
            topic="ordine_sospeso",
            ordine=ordine,
            extra_payload={
                "azione": "sospensione",
                "utente": _current_username(),
                "causale": causale,
                "elapsed_seconds": elapsed_seconds,
                "tempo_funzionamento": tempo_funzionamento,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine sospeso"

    elif stato_norm == "in sospeso":
        message = "Ordine già in sospeso"

    else:
        message = f"Ordine non sospendibile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "tempo_funzionamento": tempo_funzionamento,
                "elapsed_seconds": elapsed_seconds,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/montaggio/macchina/sospendi")
@login_required
@require_perm("home")
def api_sospendi_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    causale = _norm_text(data.get("causale"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Ordine non appartenente alla vista montaggio",
                }
            ),
            400,
        )

    if _norm_text(ordine.GestioneMatricola).lower() != "si":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questa modalità è riservata agli ordini macchina",
                }
            ),
            400,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()

    changed = False
    message = None
    elapsed_seconds = 0
    tempo_funzionamento = "0"

    if stato_norm == "attivo":
        now_dt = _now_rome_dt()
        ordine.StatoOrdine = "In Sospeso"

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine macchina. "
                            "La sospensione non può aggiornare Tempo_funzionamento."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        stato.Stato_odp = "In Sospeso"
        stato.Utente_operazione = _current_username()

        elapsed_seconds = _accumulate_runtime_until(stato, now_dt)
        stato.Stato_odp = "In Sospeso"
        tempo_funzionamento = _norm_text(stato.Tempo_funzionamento) or "0"

        _push_change_event(
            topic="ordine_sospeso_montaggio_macchina",
            ordine=ordine,
            extra_payload={
                "azione": "sospensione_macchina",
                "utente": _current_username(),
                "causale": causale,
                "matricola": matricola,
                "fase": fase,
                "elapsed_seconds": elapsed_seconds,
                "tempo_funzionamento": tempo_funzionamento,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine macchina sospeso"

    elif stato_norm == "in sospeso":
        message = "Ordine macchina già in sospeso"

    else:
        message = f"Ordine macchina non sospendibile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "tempo_funzionamento": tempo_funzionamento,
                "elapsed_seconds": elapsed_seconds,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/riattiva")
@login_required
@require_perm("home")
def api_riattiva_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)
    fase_corrente = _fase_corrente_for_export(ordine)

    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Riattivazione bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_change_event_id(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()

        ordine.StatoOrdine = "Attivo"
        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        _push_change_event(
            topic="ordine_riattivato",
            ordine=ordine,
            extra_payload={
                "azione": "riattivazione",
                "utente": _current_username(),
                "fase": fase_corrente,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
                "tempo_funzionamento": stato.Tempo_funzionamento,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine riattivato"

    elif stato_norm == "attivo":
        message = "Ordine già attivo"

    else:
        message = f"Ordine non riattivabile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "fase": fase_corrente,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/montaggio/macchina/riattiva")
@login_required
@require_perm("home")
def api_riattiva_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))

    if not id_documento or not id_riga:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "IdDocumento e IdRiga sono obbligatori",
                }
            ),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Ordine non appartenente alla vista montaggio",
                }
            ),
            400,
        )

    if _norm_text(ordine.GestioneMatricola).lower() != "si":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questa modalità è riservata agli ordini macchina",
                }
            ),
            400,
        )

    fase_corrente = _fase_corrente_for_export(ordine, fase_override=fase)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )

    if blocking_outbox is not None:
        tab, fragments = _fragments_for_ordine_tab(policy, ordine)

        return (
            jsonify(
                {
                    "ok": False,
                    "changed": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "message": (
                        f"Riattivazione bloccata: fase {fase_corrente} con export "
                        f"in stato '{blocking_outbox.status}'."
                    ),
                    "id_documento": ordine.IdDocumento,
                    "id_riga": ordine.IdRiga,
                    "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                    "rif_registraz": ordine.RifRegistraz,
                    "stato_ordine": ordine.StatoOrdine,
                    "fase": fase_corrente,
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                    "active_tab": tab,
                    "last_event_id": _last_change_event_id(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()

        ordine.StatoOrdine = "Attivo"
        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        _push_change_event(
            topic="ordine_riattivato_montaggio_macchina",
            ordine=ordine,
            extra_payload={
                "azione": "riattivazione_macchina",
                "utente": _current_username(),
                "matricola": matricola,
                "fase": fase_corrente,
                "data_ultima_attivazione": stato.data_ultima_attivazione,
                "tempo_funzionamento": stato.Tempo_funzionamento,
            },
        )

        db.session.commit()
        changed = True
        message = "Ordine macchina riattivato"
    elif stato_norm == "attivo":
        message = "Ordine macchina già attivo"

    else:
        message = f"Ordine macchina non riattivabile: stato attuale '{stato_attuale}'"

    tab, fragments = _fragments_for_ordine_tab(policy, ordine)

    return (
        jsonify(
            {
                "ok": True,
                "changed": changed,
                "message": message,
                "id_documento": ordine.IdDocumento,
                "id_riga": ordine.IdRiga,
                "row_key": _row_key(ordine.IdDocumento, ordine.IdRiga),
                "rif_registraz": ordine.RifRegistraz,
                "stato_ordine": ordine.StatoOrdine,
                "fase": fase_corrente,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/chiudi")
@login_required
@require_perm("home")
def api_chiudi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    q_ok_raw = data.get("quantita_conforme") or data.get("quantita_prodotta")
    q_nok_raw = data.get("quantita_non_conforme") or data.get("quantita_scartata")
    note = _norm_text(data.get("note"))
    lotti_input = data.get("lotti") or []
    chiusura_parziale = _parse_bool_flag(data.get("chiusura_parziale"))

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    fase_corrente = _fase_corrente_for_export(ordine)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )
    if blocking_outbox is not None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine).lower()
    if stato_attuale == "pianificata":
        return (
            jsonify(
                {"ok": False, "error": "Ordine non chiudibile: è ancora Pianificata"}
            ),
            409,
        )

    try:
        q_tot = _qty_da_lavorare_decimal(ordine)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    try:
        q_ok = (
            _parse_qty_integer_decimal(q_ok_raw, "Quantità conforme")
            if q_ok_raw is not None
            else q_tot
        )
        q_nok = (
            _parse_qty_integer_decimal(q_nok_raw, "Quantità KO")
            if q_nok_raw is not None
            else Decimal("0")
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    if q_ok < 0 or q_nok < 0:
        return (
            jsonify({"ok": False, "error": "Le quantità non possono essere negative"}),
            400,
        )

    q_lavorata = q_ok + q_nok
    qty_residua = q_tot - q_lavorata
    qty_residua_text = _decimal_to_text(qty_residua)
    qty_lavorata_text = _decimal_to_text(q_lavorata)
    qty_pre_text = _qty_da_lavorare_text(ordine)

    # Regole nuove:
    # - SL totale: nessun controllo min/max quantità
    # - SL parziale: quantità lavorata > 0 e strettamente minore del totale ordine
    if chiusura_parziale:
        if q_lavorata <= 0:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Per la chiusura parziale devi indicare una quantità lavorata > 0.",
                    }
                ),
                400,
            )

        if q_lavorata >= q_tot:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Nella chiusura parziale la quantità lavorata deve essere strettamente minore della quantità totale dell'ordine.",
                    }
                ),
                400,
            )

    componenti_richiesti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
    )
    if componenti_richiesti_lotto and not lotti_input:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Per questo ordine è obbligatoria l'assegnazione dei lotti materiale prima della chiusura.",
                }
            ),
            400,
        )

    if lotti_input:
        for lotto_row in lotti_input:
            cod_art = _norm_text(lotto_row.get("CodArt"))
            rif_lotto = _norm_text(lotto_row.get("RifLottoAlfa"))
            try:
                qty = _parse_qty_integer_decimal(
                    lotto_row.get("Quantita"),
                    f"Quantità lotto {cod_art}/{rif_lotto}",
                )
            except ValueError as e:
                return (
                    jsonify({"ok": False, "error": f"Quantità lotto non valida: {e}"}),
                    400,
                )

            if not cod_art or not rif_lotto:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": "Codice e lotto obbligatori per ogni riga.",
                        }
                    ),
                    400,
                )
            if qty <= 0:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"{cod_art} lotto {rif_lotto}: quantità deve essere > 0.",
                        }
                    ),
                    400,
                )

            lotto_db = GiacenzaLotti.query.filter_by(
                CodArt=cod_art, RifLottoAlfa=rif_lotto
            ).first()
            if lotto_db is None:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"Lotto {rif_lotto} non trovato per {cod_art}.",
                        }
                    ),
                    400,
                )
            try:
                giacenza = _parse_qty_decimal(lotto_db.Giacenza)
            except ValueError:
                giacenza = Decimal("0")
            if qty > giacenza:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"{cod_art} lotto {rif_lotto}: qtà {qty} > giacenza {giacenza}.",
                        }
                    ),
                    400,
                )

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")
    lotto_prodotto = None

    if _norm_text(ordine.GestioneLotto).lower() == "si" and q_ok > 0:
        rif_lotto_prodotto = generazione_lotti(now_dt)

        parent_lotti_ok = []
        for row in lotti_input:
            esito_row = _norm_text(row.get("Esito", "ok")).lower()
            if esito_row != "ok":
                continue

            parent_lotti_ok.append(
                {
                    "CodArt": _norm_text(row.get("CodArt")),
                    "RifLottoAlfa": _norm_text(row.get("RifLottoAlfa")),
                    "Quantita": str(row.get("Quantita", 0)),
                }
            )

        lotto_prodotto = {
            "CodArt": ordine.CodArt,
            "RifLottoAlfa": rif_lotto_prodotto,
            "Quantita": _decimal_to_text(q_ok),
            "Fase": fase_corrente,
            "ParentLotti": parent_lotti_ok,
        }

    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento, IdRiga=ordine.IdRiga
    ).first()

    tempo_finale = "0"
    if stato is not None:
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            _accumulate_runtime_until(stato, now_dt)
        tempo_finale = _norm_text(stato.Tempo_funzionamento) or "0"

    fase_corrente = _fase_corrente_for_export(ordine, stato=stato)
    outbox = None

    if chiusura_parziale:
        if stato is None:
            stato = InputOdpRuntime(
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
                RifRegistraz=ordine.RifRegistraz,
                Stato_odp="In Sospeso",
                Data_in_carico=now_iso,
                Tempo_funzionamento=tempo_finale or "0",
                Utente_operazione=_current_username(),
                FaseAttiva=fase_corrente,
                data_ultima_attivazione=None,
            )
            ordine.runtime_row = stato
            db.session.add(stato)
        else:
            stato.Stato_odp = "In Sospeso"
            stato.Utente_operazione = _current_username()

            stato.FaseAttiva = fase_corrente
            stato.data_ultima_attivazione = None

        ordine.StatoOrdine = "In Sospeso"
        ordine.QtyDaLavorare = qty_residua_text

        payload = _build_phase_payload(
            ordine=ordine,
            fase_corrente=fase_corrente,
            q_ok=q_ok,
            q_nok=q_nok,
            tempo_finale=tempo_finale,
            lotti_input=lotti_input,
            lotto_prodotto=lotto_prodotto,
            note=note,
            now_iso=now_iso,
            chiusura_parziale=True,
        )
        outbox = _queue_phase_export(
            ordine=ordine,
            fase_corrente=fase_corrente,
            payload=payload,
        )

        _push_change_event(
            topic="fase_consuntivata_parziale",
            ordine=ordine,
            extra_payload={
                "azione": "consuntivo_fase_parziale",
                "utente": _current_username(),
                "fase": fase_corrente,
                "quantita_conforme": str(q_ok),
                "quantita_non_conforme": str(q_nok),
                "quantita_lavorata_step": qty_lavorata_text,
                "qty_da_lavorare_pre": qty_pre_text,
                "qty_da_lavorare_post": qty_residua_text,
                "tempo_funzionamento": tempo_finale,
                "lotti_count": len(lotti_input),
                "chiusura_parziale": True,
                "richiede_export_erp": True,
                "erp_export_kind": "consuntivo_fase",
                "erp_outbox_flag_only": False,
                "outbox_id": outbox.outbox_id,
                "export_status": outbox.status,
                "lotto_prodotto": lotto_prodotto,
            },
        )
    else:
        payload = _build_phase_payload(
            ordine=ordine,
            fase_corrente=fase_corrente,
            q_ok=q_ok,
            q_nok=q_nok,
            tempo_finale=tempo_finale,
            lotti_input=lotti_input,
            lotto_prodotto=lotto_prodotto,
            note=note,
            now_iso=now_iso,
            chiusura_parziale=False,
        )
        outbox = _queue_phase_export(
            ordine=ordine,
            fase_corrente=fase_corrente,
            payload=payload,
        )

        _push_change_event(
            topic="fase_consuntivata",
            ordine=ordine,
            extra_payload={
                "azione": "consuntivo_fase",
                "utente": _current_username(),
                "fase": fase_corrente,
                "quantita_conforme": str(q_ok),
                "quantita_non_conforme": str(q_nok),
                "tempo_funzionamento": tempo_finale,
                "lotti_count": len(lotti_input),
                "outbox_id": outbox.outbox_id,
                "export_status": outbox.status,
                "chiusura_parziale": False,
                "lotto_prodotto": lotto_prodotto,
            },
        )

    db.session.flush()

    note_chiusura_log = note
    if chiusura_parziale:
        note_chiusura_log = (
            f"[PARZIALE] residuo={qty_residua_text}; {note}".strip().rstrip(";")
        )

    transition = _advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        qty_residua=qty_residua,
        qty_residua_text=qty_residua_text,
        qty_lavorata_text=qty_lavorata_text,
        chiusura_parziale=chiusura_parziale,
        username=_current_username(),
    )

    tab = _tab_from_ordine(ordine)
    stato_ordine_response = ordine.StatoOrdine
    qty_da_lavorare_response = _norm_text(ordine.QtyDaLavorare)
    db.session.add(
        InputOdpLog(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=ordine.CodArt,
            DesArt=ordine.DesArt,
            Quantita=ordine.Quantita,
            NumFase=ordine.NumFase,
            CodLavorazione=ordine.CodLavorazione,
            CodRisorsaProd=ordine.CodRisorsaProd,
            DataInizioSched=ordine.DataInizioSched,
            DataFineSched=ordine.DataFineSched,
            GestioneLotto=ordine.GestioneLotto,
            GestioneMatricola=ordine.GestioneMatricola,
            DistintaMateriale=ordine.DistintaMateriale,
            CodMatricola=ordine.CodMatricola,
            StatoRiga=ordine.StatoRiga,
            CodFamiglia=ordine.CodFamiglia,
            CodMacrofamiglia=ordine.CodMacrofamiglia,
            CodMagPrincipale=ordine.CodMagPrincipale,
            CodReparto=ordine.CodReparto,
            TempoPrevistoLavoraz=ordine.TempoPrevistoLavoraz,
            StatoOrdine=ordine.StatoOrdine,
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=ordine.FaseAttiva,
            Note=ordine.Note,
            QtyDaLavorare=_norm_text(ordine.QtyDaLavorare),
            RisorsaAttiva=_norm_text(ordine.RisorsaAttiva),
            LavorazioneAttiva=_norm_text(ordine.LavorazioneAttiva),
            QuantitaConforme=str(q_ok),
            QuantitaNonConforme=str(q_nok),
            NoteChiusura=note_chiusura_log,
            ClosedBy=_current_username(),
            ClosedAt=now_iso,
        )
    )

    if stato is not None:
        db.session.add(
            StatoOdpLog(
                IdDocumento=stato.IdDocumento,
                IdRiga=stato.IdRiga,
                RifRegistraz=stato.RifRegistraz,
                Stato_odp=stato.Stato_odp,
                Data_in_carico=stato.Data_in_carico,
                Tempo_funzionamento=tempo_finale,
                Utente_operazione=stato.Utente_operazione,
                Fase=getattr(stato, "FaseAttiva", None),
                data_ultima_attivazione=stato.data_ultima_attivazione,
                ClosedBy=_current_username(),
                ClosedAt=now_iso,
            )
        )

    if lotti_input:
        for lotto_row in lotti_input:
            lotto_log = LottiUsatiLog(
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
                RifRegistraz=ordine.RifRegistraz,
                CodArt=_norm_text(lotto_row.get("CodArt")),
                RifLottoAlfa=_norm_text(lotto_row.get("RifLottoAlfa")),
                Quantita=str(lotto_row.get("Quantita", 0)),
                Esito=_norm_text(lotto_row.get("Esito", "ok")),
                ClosedBy=_current_username(),
                ClosedAt=now_iso,
            )
            if hasattr(LottiUsatiLog, "Fase"):
                lotto_log.Fase = fase_corrente
            db.session.add(lotto_log)
    if lotto_prodotto is not None:
        gen_etichette(
            str(lotto_prodotto["CodArt"]),
            ordine.DesArt,
            str(lotto_prodotto["RifLottoAlfa"]),
            ordine.Quantita,
            current_app.config["DIMENSIONI"],
            current_app.config["DPI"],
            current_app.config["FONT_PATH"],
        )
        db.session.add(
            LottiGeneratiLog(
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
                RifRegistraz=ordine.RifRegistraz,
                CodArt=lotto_prodotto["CodArt"],
                RifLottoAlfa=lotto_prodotto["RifLottoAlfa"],
                Quantita=lotto_prodotto["Quantita"],
                Fase=lotto_prodotto["Fase"],
                ParentLottiJson=json.dumps(
                    lotto_prodotto["ParentLotti"], ensure_ascii=False
                ),
                ClosedBy=_current_username(),
                ClosedAt=now_iso,
            )
        )

    query = getattr(ChangeEvent, "query", None)

    if query is not None and hasattr(query, "filter") and hasattr(query, "order_by"):
        ce_rows = (
            query.filter(ChangeEvent.payload_json.isnot(None))
            .filter(
                func.json_extract(ChangeEvent.payload_json, "$.id_documento")
                == ordine.IdDocumento
            )
            .filter(
                func.json_extract(ChangeEvent.payload_json, "$.id_riga")
                == ordine.IdRiga
            )
            .order_by(ChangeEvent.id)
            .all()
        )
    else:
        ce_rows = []
    for ce in ce_rows:
        db.session.add(
            ChangeEventLog(
                src_id=ce.id,
                topic=ce.topic,
                scope=ce.scope,
                payload_json=ce.payload_json,
                created_at=ce.created_at,
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
            )
        )

    if transition["tipo"] == "finale":
        _delete_closed_order_from_runtime_db(ordine=ordine, stato=stato)
        stato_ordine_response = "Chiusa"
        qty_da_lavorare_response = "0"
    fragments = {}
    if tab:
        reparto_code = BRIDGE_CONFIG[tab]["reparto"]
        odp = list(_query_for_tab(policy, reparto_code).all())
        fragments = RENDERERS[tab](odp)

    db.session.commit()

    if transition["tipo"] == "finale":
        message = (
            "Ordine chiuso definitivamente, archiviato nel db_log "
            "e rimosso dal database operativo."
        )
    elif transition["tipo"] == "avanzata":
        message = (
            f"Fase {transition['fase_corrente']} consuntivata. "
            f"File TXT generato in coda export. "
            f"Ordine mantenuto a DB e riportato in pianificata sulla fase "
            f"{transition['fase_successiva']}."
        )
    else:
        message = (
            f"Fase {transition['fase_corrente']} chiusa parzialmente. "
            f"File TXT generato in coda export. "
            f"Ordine mantenuto a DB e messo in sospeso sulla stessa fase."
        )

    return (
        jsonify(
            {
                "ok": True,
                "changed": True,
                "message": message,
                "id_documento": id_documento,
                "id_riga": id_riga,
                "row_key": _row_key(id_documento, id_riga),
                "fase": transition["fase_corrente"],
                "fase_successiva": transition["fase_successiva"],
                "stato_ordine": stato_ordine_response,
                "qty_da_lavorare": qty_da_lavorare_response,
                "outbox_id": outbox.outbox_id if outbox else None,
                "outbox_status": outbox.status if outbox else None,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/montaggio/macchina/chiudi")
@login_required
@require_perm("home")
def api_chiudi_ordine_montaggio_macchina():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    matricola = _norm_text(data.get("matricola"))
    fase = _norm_text(data.get("fase"))
    note = _norm_text(data.get("note"))
    lotti_input = data.get("lotti") or []

    if not id_documento or not id_riga:
        return (
            jsonify({"ok": False, "error": "IdDocumento e IdRiga sono obbligatori"}),
            400,
        )

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if _tab_from_ordine(ordine) != "montaggio":
        return (
            jsonify(
                {"ok": False, "error": "Ordine non appartenente alla vista montaggio"}
            ),
            400,
        )

    if _norm_text(ordine.GestioneMatricola).lower() != "si":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questa modalità è riservata agli ordini macchina",
                }
            ),
            400,
        )

    fase_corrente = _fase_corrente_for_export(ordine, fase_override=fase)
    blocking_outbox = _get_blocking_outbox_for_phase(
        id_documento=ordine.IdDocumento,
        id_riga=ordine.IdRiga,
        fase=fase_corrente,
    )
    if blocking_outbox is not None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Questa fase risulta già consuntivata ed è ancora in attesa "
                        "di sincronizzazione con il gestionale."
                    ),
                    "outbox_status": blocking_outbox.status,
                    "outbox_id": blocking_outbox.outbox_id,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine).lower()
    if stato_attuale == "pianificata":
        return (
            jsonify(
                {"ok": False, "error": "Ordine non chiudibile: è ancora Pianificata"}
            ),
            409,
        )

    componenti_richiesti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
    )
    if componenti_richiesti_lotto and not lotti_input:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Per questo ordine è obbligatoria l'assegnazione dei lotti materiale prima della chiusura.",
                }
            ),
            400,
        )

    if lotti_input:
        for lotto_row in lotti_input:
            cod_art = _norm_text(lotto_row.get("CodArt"))
            rif_lotto = _norm_text(lotto_row.get("RifLottoAlfa"))
            try:
                qty = _parse_qty_decimal(lotto_row.get("Quantita"))
            except ValueError as e:
                return jsonify(
                    {"ok": False, "error": f"Quantità lotto non valida: {e}"}
                ), 400

            if not cod_art or not rif_lotto:
                return jsonify(
                    {
                        "ok": False,
                        "error": "Codice e lotto sono obbligatori per ogni riga lotti.",
                    }
                ), 400

            if qty <= 0:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"{cod_art} lotto {rif_lotto}: quantità deve essere > 0.",
                    }
                ), 400

            lotto_db = GiacenzaLotti.query.filter_by(
                CodArt=cod_art, RifLottoAlfa=rif_lotto
            ).first()

            if lotto_db is None:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"Lotto {rif_lotto} non trovato per {cod_art}.",
                    }
                ), 400

            try:
                giacenza = _parse_qty_decimal(lotto_db.Giacenza)
            except ValueError:
                giacenza = Decimal("0")

            if qty > giacenza:
                return jsonify(
                    {
                        "ok": False,
                        "error": f"{cod_art} lotto {rif_lotto}: quantità {qty} supera giacenza {giacenza}.",
                    }
                ), 400

    try:
        q_tot = _qty_da_lavorare_decimal(ordine)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    q_ok = q_tot
    q_nok = Decimal("0")
    qty_residua = Decimal("0")
    qty_residua_text = "0"
    qty_lavorata_text = _decimal_to_text(q_tot)
    chiusura_parziale = False

    now_dt = _now_rome_dt()
    now_iso = now_dt.isoformat(timespec="seconds")

    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento, IdRiga=ordine.IdRiga
    ).first()

    tempo_finale = "0"
    if stato is not None:
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            _accumulate_runtime_until(stato, now_dt)
        tempo_finale = _norm_text(stato.Tempo_funzionamento) or "0"

    fase_corrente = _fase_corrente_for_export(ordine, stato=stato, fase_override=fase)
    payload = _build_phase_payload(
        ordine=ordine,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        tempo_finale=tempo_finale,
        lotti_input=lotti_input,
        lotto_prodotto=None,
        note=note,
        now_iso=now_iso,
    )

    outbox = _queue_phase_export(
        ordine=ordine,
        fase_corrente=fase_corrente,
        payload=payload,
    )

    _push_change_event(
        topic="fase_consuntivata_montaggio_macchina",
        ordine=ordine,
        extra_payload={
            "azione": "consuntivo_fase_macchina",
            "utente": _current_username(),
            "matricola": matricola,
            "fase": fase_corrente,
            "quantita_conforme": str(q_ok),
            "quantita_non_conforme": str(q_nok),
            "tempo_funzionamento": tempo_finale,
            "lotti_count": len(lotti_input),
            "outbox_id": outbox.outbox_id,
            "export_status": outbox.status,
        },
    )
    db.session.flush()

    transition = _advance_or_finalize_phase(
        ordine=ordine,
        stato=stato,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        qty_residua=qty_residua,
        qty_residua_text=qty_residua_text,
        qty_lavorata_text=qty_lavorata_text,
        chiusura_parziale=chiusura_parziale,
        username=_current_username(),
    )

    tab = _tab_from_ordine(ordine)
    stato_ordine_response = ordine.StatoOrdine
    qty_da_lavorare_response = _norm_text(ordine.QtyDaLavorare)

    db.session.add(
        InputOdpLog(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=ordine.CodArt,
            DesArt=ordine.DesArt,
            Quantita=ordine.Quantita,
            NumFase=ordine.NumFase,
            CodLavorazione=ordine.CodLavorazione,
            CodRisorsaProd=ordine.CodRisorsaProd,
            DataInizioSched=ordine.DataInizioSched,
            DataFineSched=ordine.DataFineSched,
            GestioneLotto=ordine.GestioneLotto,
            GestioneMatricola=ordine.GestioneMatricola,
            DistintaMateriale=ordine.DistintaMateriale,
            CodMatricola=ordine.CodMatricola,
            StatoRiga=ordine.StatoRiga,
            CodFamiglia=ordine.CodFamiglia,
            CodMacrofamiglia=ordine.CodMacrofamiglia,
            CodMagPrincipale=ordine.CodMagPrincipale,
            CodReparto=ordine.CodReparto,
            TempoPrevistoLavoraz=ordine.TempoPrevistoLavoraz,
            StatoOrdine=ordine.StatoOrdine,
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=ordine.FaseAttiva,
            Note=ordine.Note,
            QuantitaConforme=str(q_ok),
            QuantitaNonConforme=str(q_nok),
            NoteChiusura=note,
            ClosedBy=_current_username(),
            ClosedAt=now_iso,
            QtyDaLavorare=_norm_text(ordine.QtyDaLavorare),
            RisorsaAttiva=_norm_text(ordine.RisorsaAttiva),
            LavorazioneAttiva=_norm_text(ordine.LavorazioneAttiva),
        )
    )

    if stato is not None:
        db.session.add(
            StatoOdpLog(
                IdDocumento=stato.IdDocumento,
                IdRiga=stato.IdRiga,
                RifRegistraz=stato.RifRegistraz,
                Stato_odp=stato.Stato_odp,
                Data_in_carico=stato.Data_in_carico,
                Tempo_funzionamento=tempo_finale,
                Utente_operazione=stato.Utente_operazione,
                Fase=getattr(stato, "FaseAttiva", None),
                data_ultima_attivazione=stato.data_ultima_attivazione,
                ClosedBy=_current_username(),
                ClosedAt=now_iso,
            )
        )

    for lotto_row in lotti_input:
        lotto_log = LottiUsatiLog(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=_norm_text(lotto_row.get("CodArt")),
            RifLottoAlfa=_norm_text(lotto_row.get("RifLottoAlfa")),
            Quantita=str(lotto_row.get("Quantita", 0)),
            Esito=_norm_text(lotto_row.get("Esito", "ok")),
            ClosedBy=_current_username(),
            ClosedAt=now_iso,
        )
        if hasattr(LottiUsatiLog, "Fase"):
            lotto_log.Fase = fase_corrente
        db.session.add(lotto_log)

    query = getattr(ChangeEvent, "query", None)

    if query is not None and hasattr(query, "filter") and hasattr(query, "order_by"):
        ce_rows = (
            query.filter(ChangeEvent.payload_json.isnot(None))
            .filter(
                func.json_extract(ChangeEvent.payload_json, "$.id_documento")
                == ordine.IdDocumento
            )
            .filter(
                func.json_extract(ChangeEvent.payload_json, "$.id_riga")
                == ordine.IdRiga
            )
            .order_by(ChangeEvent.id)
            .all()
        )
    else:
        ce_rows = []
    for ce in ce_rows:
        db.session.add(
            ChangeEventLog(
                src_id=ce.id,
                topic=ce.topic,
                scope=ce.scope,
                payload_json=ce.payload_json,
                created_at=ce.created_at,
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
            )
        )
    fragments = {}
    if tab:
        reparto_code = BRIDGE_CONFIG[tab]["reparto"]
        odp = list(_query_for_tab(policy, reparto_code).all())
        fragments = RENDERERS[tab](odp)

    db.session.commit()

    if transition["tipo"] == "finale":
        _delete_closed_order_from_runtime_db(ordine=ordine, stato=stato)
        stato_ordine_response = "Chiusa"
        qty_da_lavorare_response = "0"
    else:
        message = (
            f"Fase macchina {transition['fase_corrente']} consuntivata. "
            f"File TXT generato in coda export. "
            f"Ordine mantenuto a DB e riportato in pianificata sulla fase "
            f"{transition['fase_successiva']}."
        )

    return (
        jsonify(
            {
                "ok": True,
                "changed": True,
                "message": message,
                "id_documento": id_documento,
                "id_riga": id_riga,
                "row_key": _row_key(id_documento, id_riga),
                "fase": transition["fase_corrente"],
                "fase_successiva": transition["fase_successiva"],
                "stato_ordine": stato_ordine_response,
                "qty_da_lavorare": qty_da_lavorare_response,
                "outbox_id": outbox.outbox_id,
                "outbox_status": outbox.status,
                "active_tab": tab,
                "last_event_id": _last_change_event_id(),
                "fragments": fragments,
            }
        ),
        200,
    )


@main_bp.post("/api/ordini/lotti-componenti")
@login_required
@require_perm("home")
def api_lotti_componenti():
    data = request.get_json(silent=True) or {}
    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    modalita = _norm_text(data.get("modalita")).lower()
    is_macchina = modalita == "m"

    if not id_documento or not id_riga:
        return jsonify({"ok": False, "error": "IdDocumento e IdRiga obbligatori"}), 400

    policy = RbacPolicy(current_user)
    ordine = _get_visible_odp_by_key(policy, id_documento, id_riga)

    if is_macchina:
        componenti_lotto = _componenti_lotto_per_ordine(
            ordine,
            include_senza_lotti=True,
            ignore_parent_gestione_lotto=True,
        )

        return jsonify(
            {
                "ok": True,
                "gestioneLotto": True,
                "force_show_section": len(componenti_lotto) > 0,
                "haComponentiLotto": any(
                    isinstance(c.get("lotti"), list) and len(c["lotti"]) > 0
                    for c in componenti_lotto
                ),
                "componenti": componenti_lotto,
            }
        )

    ordine_gestione_lotto = _norm_text(ordine.GestioneLotto).lower() == "si"

    if not ordine_gestione_lotto:
        return jsonify(
            {
                "ok": True,
                "gestioneLotto": False,
                "haComponentiLotto": False,
                "componenti": [],
            }
        )

    componenti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
    )

    return jsonify(
        {
            "ok": True,
            "gestioneLotto": True,
            "force_show_section": len(componenti_lotto) > 0,
            "haComponentiLotto": any(
                isinstance(c.get("lotti"), list) and len(c["lotti"]) > 0
                for c in componenti_lotto
            ),
            "componenti": componenti_lotto,
        }
    )


def generazione_lotti(dt=None) -> str:
    dt = dt or _now_rome_dt()
    return dt.strftime("%Y%m%d")


@main_bp.post("/api/erp/export/avp")
@login_required
@require_perm("home")
def api_export_avp_txt():
    data = request.get_json(silent=True) or {}
    suffix = _norm_text(data.get("suffix")) or "manuale"

    outbox_rows = _get_pending_avp_outbox()
    if not outbox_rows:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Nessun record ERP pending da esportare",
                }
            ),
            404,
        )

    try:
        content = _build_avp_txt_content(outbox_rows)
        path_txt = _write_txt_content(
            content,
            prefix="AVPB",
            suffix=suffix,
            encoding="utf-8-sig",
        )

        now_iso = _now_rome_dt().isoformat(timespec="seconds")
        for row in outbox_rows:
            row.status = "exported"
            row.exported_at = now_iso
            row.last_error = None
            row.attempts = int(row.attempts or 0) + 1

        db.session.commit()

        return jsonify(
            {
                "ok": True,
                "message": "File AVP generato correttamente",
                "file_name": path_txt.name,
                "file_path": str(path_txt),
                "records": len(outbox_rows),
            }
        )
    except Exception as exc:
        err = str(exc)

        try:
            for row in outbox_rows:
                row.status = "error"
                row.last_error = err
                row.attempts = int(row.attempts or 0) + 1
            db.session.commit()
        except Exception:
            db.session.rollback()

        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Errore generazione file AVP: {err}",
                }
            ),
            500,
        )
