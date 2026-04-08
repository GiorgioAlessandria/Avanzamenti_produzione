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
from sqlalchemy import func, select, delete
from app_odp.etichette import gen_etichette
from app_odp.models import (
    InputOdp,
    InputOdpRuntime,
    db,
    Causaliattivita,
    GiacenzaLotti,
    LottiUsatiLog,
    ErpOutbox,
    InputOdpLog,
    OdpRuntimeLog,
    LottiGeneratiLog,
    Roles,
    Reparti,
    User,
    user_roles,
    users_lavorazioni,
    users_risorse,
    Permissions,
    Risorse,
    Lavorazioni,
    Magazzini,
    Famiglia,
    Macrofamiglia,
    roles_permission,
    roles_reparti,
    roles_risorse,
    roles_lavorazioni,
    roles_magazzini,
    roles_famiglia,
    roles_macrofamiglia,
    roles_ineritance,
    roles_manageable_roles,
)
from app_odp.policy.decorator import require_perm
from app_odp.policy.policy import RbacPolicy
from app_odp.odp_output import txt_generator, DEFAULT_AVP_CFG


try:
    from icecream import ic
finally:
    pass

main_bp = Blueprint("main", __name__)
ROME_TZ = ZoneInfo("Europe/Rome")


# region FUNZIONI
ROLE_LINK_CONFIG = {
    "permissions": {
        "label": "Permessi",
        "assoc_table": roles_permission,
        "left_fk": "role_id",
        "right_fk": "permission_id",
        "model": Permissions,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "reparti": {
        "label": "Reparti",
        "assoc_table": roles_reparti,
        "left_fk": "roles_id",
        "right_fk": "reparto_id",
        "model": Reparti,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "risorse": {
        "label": "Risorse",
        "assoc_table": roles_risorse,
        "left_fk": "roles_id",
        "right_fk": "risorse_id",
        "model": Risorse,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "lavorazioni": {
        "label": "Lavorazioni",
        "assoc_table": roles_lavorazioni,
        "left_fk": "roles_id",
        "right_fk": "lavorazioni_id",
        "model": Lavorazioni,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "magazzini": {
        "label": "Magazzini",
        "assoc_table": roles_magazzini,
        "left_fk": "roles_id",
        "right_fk": "magazzini_id",
        "model": Magazzini,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "famiglia": {
        "label": "Famiglia",
        "assoc_table": roles_famiglia,
        "left_fk": "roles_id",
        "right_fk": "famiglia_id",
        "model": Famiglia,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "macrofamiglia": {
        "label": "Macrofamiglia",
        "assoc_table": roles_macrofamiglia,
        "left_fk": "roles_id",
        "right_fk": "macrofamiglia_id",
        "model": Macrofamiglia,
        "model_id": "id",
        "code_attr": "Codice",
        "desc_attr": "Descrizione",
    },
    "ruoli_ereditati": {
        "label": "Ruoli ereditati",
        "assoc_table": roles_ineritance,
        "left_fk": "role_id",
        "right_fk": "included_role",
        "model": Roles,
        "model_id": "id",
        "code_attr": "name",
        "desc_attr": "description",
    },
    "ruoli_gestibili": {
        "label": "Ruoli gestibili",
        "assoc_table": roles_manageable_roles,
        "left_fk": "manager_role_id",
        "right_fk": "managed_role_id",
        "model": Roles,
        "model_id": "id",
        "code_attr": "name",
        "desc_attr": "description",
    },
}


def _new_dash_kpi_bucket() -> dict:
    return {
        "totali": 0,
        "pianificati": 0,
        "sospesi": 0,
        "attivi": 0,
        "ore_lavorazione": 0.0,
        "ore_attrezzaggio": 0.0,
        "percentuale_attivi": 0.0,
        "percentuale_sospesi": 0.0,
    }


def _finalize_dash_kpi(bucket: dict) -> dict:
    totali = int(bucket.get("totali", 0) or 0)
    attivi = int(bucket.get("attivi", 0) or 0)
    sospesi = int(bucket.get("sospesi", 0) or 0)
    bucket["ore_lavorazione"] = round(
        float(bucket.get("ore_lavorazione", 0.0) or 0.0), 2
    )
    bucket["ore_attrezzaggio"] = round(
        float(bucket.get("ore_attrezzaggio", 0.0) or 0.0),
        2,
    )
    if totali > 0:
        bucket["percentuale_attivi"] = round((attivi / totali) * 100, 2)
        bucket["percentuale_sospesi"] = round((sospesi / totali) * 100, 2)
    else:
        bucket["percentuale_attivi"] = 0.0
        bucket["percentuale_sospesi"] = 0.0
    return bucket


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


def _last_log_token() -> str:
    runtime_max = db.session.query(func.max(OdpRuntimeLog.log_id)).scalar() or 0
    input_max = db.session.query(func.max(InputOdpLog.log_id)).scalar() or 0
    return f"{input_max}:{runtime_max}"


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


def _qty_da_lavorare_text(ordine, stato=None) -> str:
    if stato is not None:
        qty_runtime = _norm_text(getattr(stato, "QtyDaLavorare", ""))
        if qty_runtime:
            return qty_runtime

    return _norm_text(getattr(ordine, "QtyDaLavorare", "")) or _norm_text(
        ordine.Quantita
    )


def _qty_da_lavorare_decimal(ordine, stato=None) -> Decimal:
    return _parse_qty_decimal(_qty_da_lavorare_text(ordine, stato=stato))


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


def _set_runtime_sospeso(
    stato,
    username: str,
    fase_corrente: str,
    qty_residua_text: str = "",
):
    if stato is None:
        return
    stato.Stato_odp = "In Sospeso"
    stato.Utente_operazione = username
    if fase_corrente:
        stato.FaseAttiva = fase_corrente
    if qty_residua_text != "":
        stato.QtyDaLavorare = qty_residua_text
    stato.data_ultima_attivazione = None


def _safe_float(value) -> float:
    raw = _norm_text(value).replace(",", ".")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _order_hours_snapshot_complessiva(ordine: InputOdp) -> tuple[float, float]:
    fase_attiva = _norm_text(getattr(ordine, "FaseAttiva", "")) or "1"
    ore_lavorazione = InputOdp._active_value_from_phase_list(
        getattr(ordine, "TempoPrevistoLavoraz", ""),
        fase_attiva,
    )
    minuti_attrezzaggio = getattr(ordine, "AttrezzaggioAttivo", "")

    ore_lavorazione_val = _safe_float(ore_lavorazione)
    minuti_attrezzaggio_val = _safe_float(minuti_attrezzaggio)
    ore_attrezzaggio_val = minuti_attrezzaggio_val / 60.0

    return ore_lavorazione_val, ore_attrezzaggio_val


def _order_hours_snapshot_reparto(ordine: InputOdp) -> float:
    fase_attiva = _norm_text(getattr(ordine, "FaseAttiva", "")) or "1"
    ore_lavorazione = InputOdp._active_value_from_phase_list(
        getattr(ordine, "TempoPrevistoLavoraz", ""),
        fase_attiva,
    )
    ore_lavorazione_val = _safe_float(ore_lavorazione)

    return ore_lavorazione_val


def _dash_complessiva_new_bucket() -> dict:
    return {
        "totali": 0,
        "pianificati": 0,
        "sospesi": 0,
        "attivi": 0,
        "ore_ordini_pianificati": 0.0,
        "ore_ordini_attivi": 0.0,
        "ore_ordini_sospesi": 0.0,
        "ore_attrezzaggio_ordini_pianificati": 0.0,
        "ore_attrezzaggio_ordini_attivi": 0.0,
        "ore_attrezzaggio_ordini_sospesi": 0.0,
        "giorni_impegno_attivi": 0.0,
        "giorni_impegno_sospesi": 0.0,
    }


def _dash_complessiva_finalize_bucket(bucket: dict) -> dict:
    bucket["ore_ordini_pianificati"] = round(
        float(bucket.get("ore_ordini_pianificati", 0.0) or 0.0),
        2,
    )
    bucket["ore_ordini_attivi"] = round(
        float(bucket.get("ore_ordini_attivi", 0.0) or 0.0),
        2,
    )
    bucket["ore_ordini_sospesi"] = round(
        float(bucket.get("ore_ordini_sospesi", 0.0) or 0.0),
        2,
    )
    bucket["ore_attrezzaggio_ordini_pianificati"] = round(
        float(bucket.get("ore_attrezzaggio_ordini_pianificati", 0.0) or 0.0),
        2,
    )
    bucket["ore_attrezzaggio_ordini_attivi"] = round(
        float(bucket.get("ore_attrezzaggio_ordini_attivi", 0.0) or 0.0),
        2,
    )
    bucket["ore_attrezzaggio_ordini_sospesi"] = round(
        float(bucket.get("ore_attrezzaggio_ordini_sospesi", 0.0) or 0.0),
        2,
    )
    ore_tot_attivi = float(bucket.get("ore_ordini_attivi", 0.0) or 0.0) + float(
        bucket.get("ore_attrezzaggio_ordini_attivi", 0.0) or 0.0
    )
    ore_tot_sospesi = float(bucket.get("ore_ordini_sospesi", 0.0) or 0.0) + float(
        bucket.get("ore_attrezzaggio_ordini_sospesi", 0.0) or 0.0
    )
    bucket["giorni_impegno_attivi"] = _hours_to_work_days(ore_tot_attivi)
    bucket["giorni_impegno_sospesi"] = _hours_to_work_days(ore_tot_sospesi)
    return bucket


def _dash_complessiva_apply_order(
    bucket: dict,
    stato: str,
    ore_lavorazione: float,
    ore_attrezzaggio: float,
) -> None:
    bucket["totali"] += 1

    if "pianificat" in stato:
        bucket["pianificati"] += 1
        bucket["ore_ordini_pianificati"] += ore_lavorazione
        bucket["ore_attrezzaggio_ordini_pianificati"] += ore_attrezzaggio
    elif "sospes" in stato:
        bucket["sospesi"] += 1
        bucket["ore_ordini_sospesi"] += ore_lavorazione
        bucket["ore_attrezzaggio_ordini_sospesi"] += ore_attrezzaggio
    elif "attiv" in stato:
        bucket["attivi"] += 1
        bucket["ore_ordini_attivi"] += ore_lavorazione
        bucket["ore_attrezzaggio_ordini_attivi"] += ore_attrezzaggio


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
        _set_runtime_sospeso(
            stato,
            username,
            fase_corrente,
            qty_residua_text=qty_residua_text,
        )
        return {
            "tipo": "parziale_stessa_fase",
            "fase_corrente": fase_corrente,
            "fase_successiva": fase_corrente,
        }

    if is_last_phase:
        ordine.StatoOrdine = "Chiusa"
        ordine.FaseAttiva = fase_corrente
        ordine.QtyDaLavorare = "0"
        _sync_active_fields_for_phase(ordine, fase_corrente)
        return {
            "tipo": "finale",
            "fase_corrente": fase_corrente,
            "fase_successiva": None,
        }

    ordine.StatoOrdine = "Pianificata"
    ordine.FaseAttiva = next_phase
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
    ordine.AttrezzaggioAttivo = _active_value_for_phase(
        getattr(ordine, "TempoAttrezzaggio", ""),
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

    return _norm_text(lotto.get("RifLottoAlfa"))


def _current_username(default: str = "utente_sconosciuto") -> str:
    return (
        getattr(current_user, "username", None)
        or getattr(current_user, "name", None)
        or getattr(current_user, "email", None)
        or str(getattr(current_user, "id", default))
    )


def _bool_text(value: bool) -> str:
    return "true" if bool(value) else "false"


def _build_operation_group_id(ordine, action: str, when_iso: str) -> str:
    stamp = re.sub(r"\D+", "", _norm_text(when_iso))[:14]
    if not stamp:
        stamp = _now_rome_dt().strftime("%Y%m%d%H%M%S")

    return (
        f"{stamp}_"
        f"{_safe_txt_suffix(_norm_text(ordine.IdDocumento), 'doc')}_"
        f"{_safe_txt_suffix(_norm_text(ordine.IdRiga), 'riga')}_"
        f"{_safe_txt_suffix(_norm_text(action), 'op')}"
    )


def _runtime_snapshot(stato) -> dict:
    return {
        "stato_odp": _norm_text(getattr(stato, "Stato_odp", "")),
        "fase": _norm_text(getattr(stato, "FaseAttiva", "")),
        "data_in_carico": _norm_text(getattr(stato, "Data_in_carico", "")),
        "data_ultima_attivazione": _norm_text(
            getattr(stato, "data_ultima_attivazione", "")
        ),
        "tempo_funzionamento": _norm_text(getattr(stato, "Tempo_funzionamento", "")),
        "qty_da_lavorare": _norm_text(getattr(stato, "QtyDaLavorare", "")),
        "utente_operazione": _norm_text(getattr(stato, "Utente_operazione", "")),
        "rif_ordine_princ": _norm_text(getattr(stato, "RifOrdinePrinc", "")),
    }


def _add_input_odp_closure_log(
    *,
    operation_group_id: str,
    ordine,
    fase_consuntivata: str,
    q_ok: Decimal,
    q_nok: Decimal,
    tempo_finale: str,
    minuti_non_funzionamento: int,
    secondi_non_funzionamento: int,
    chiusura_parziale: bool,
    note_chiusura: str,
    stato_ordine_pre: str,
    stato_ordine_post: str,
    qty_pre: str,
    qty_post: str,
    closed_by: str,
    closed_at: str,
):
    db.session.add(
        InputOdpLog(
            OperationGroupId=operation_group_id,
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
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=_norm_text(ordine.FaseAttiva),
            QtyDaLavorare=_norm_text(ordine.QtyDaLavorare),
            RisorsaAttiva=_norm_text(ordine.RisorsaAttiva),
            LavorazioneAttiva=_norm_text(ordine.LavorazioneAttiva),
            AttrezzaggioAttivo=_norm_text(ordine.AttrezzaggioAttivo),
            RifOrdinePrinc=_norm_text(getattr(ordine, "RifOrdinePrinc", "")),
            Note=ordine.Note,
            FaseConsuntivata=_norm_text(fase_consuntivata),
            QuantitaConforme=str(q_ok),
            QuantitaNonConforme=str(q_nok),
            TempoFunzionamentoFinale=_norm_text(tempo_finale),
            TempoNonFunzionamentoMinuti=_norm_text(minuti_non_funzionamento),
            TempoNonFunzionamentoSecondi=_norm_text(secondi_non_funzionamento),
            ChiusuraParziale=_bool_text(chiusura_parziale),
            NoteChiusura=_norm_text(note_chiusura),
            StatoOrdinePre=_norm_text(stato_ordine_pre),
            StatoOrdinePost=_norm_text(stato_ordine_post),
            QtyDaLavorarePre=_norm_text(qty_pre),
            QtyDaLavorarePost=_norm_text(qty_post),
            ClosedBy=_norm_text(closed_by),
            ClosedAt=_norm_text(closed_at),
        )
    )


def _add_input_odp_takeover_log(
    *,
    operation_group_id: str,
    ordine,
    stato_ordine_pre: str,
    stato_ordine_post: str,
    qty_pre: str,
    qty_post: str,
    taken_by: str,
    taken_at: str,
    note_evento: str = "Presa in carico ordine",
):
    db.session.add(
        InputOdpLog(
            OperationGroupId=operation_group_id,
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
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=_norm_text(ordine.FaseAttiva),
            QtyDaLavorare=_norm_text(ordine.QtyDaLavorare),
            RisorsaAttiva=_norm_text(ordine.RisorsaAttiva),
            LavorazioneAttiva=_norm_text(ordine.LavorazioneAttiva),
            AttrezzaggioAttivo=_norm_text(ordine.AttrezzaggioAttivo),
            RifOrdinePrinc=_norm_text(getattr(ordine, "RifOrdinePrinc", "")),
            Note=ordine.Note,
            FaseConsuntivata=None,
            QuantitaConforme=None,
            QuantitaNonConforme=None,
            TempoFunzionamentoFinale=None,
            TempoNonFunzionamentoMinuti=None,
            TempoNonFunzionamentoSecondi=None,
            ChiusuraParziale=None,
            NoteChiusura=_norm_text(note_evento),
            StatoOrdinePre=_norm_text(stato_ordine_pre),
            StatoOrdinePost=_norm_text(stato_ordine_post),
            QtyDaLavorarePre=_norm_text(qty_pre),
            QtyDaLavorarePost=_norm_text(qty_post),
            ClosedBy=_norm_text(taken_by),
            ClosedAt=_norm_text(taken_at),
        )
    )


def _add_input_odp_suspend_log(
    *,
    operation_group_id: str,
    ordine,
    stato_ordine_pre: str,
    stato_ordine_post: str,
    qty_pre: str,
    qty_post: str,
    suspended_by: str,
    suspended_at: str,
    causale: str = "",
    minuti_non_funzionamento: int | str | None = None,
    secondi_non_funzionamento: int | str | None = None,
    note_evento: str = "Sospensione ordine",
):
    note_parts = [note_evento]
    if causale:
        note_parts.append(f"Causale: {causale}")
    if minuti_non_funzionamento not in (None, ""):
        note_parts.append(
            f"Tempo non funzionamento minuti: {_norm_text(minuti_non_funzionamento)}"
        )
    if secondi_non_funzionamento not in (None, ""):
        note_parts.append(
            f"Tempo non funzionamento secondi: {_norm_text(secondi_non_funzionamento)}"
        )

    db.session.add(
        InputOdpLog(
            OperationGroupId=operation_group_id,
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
            CodClassifTecnica=ordine.CodClassifTecnica,
            CodTipoDoc=ordine.CodTipoDoc,
            FaseAttiva=_norm_text(ordine.FaseAttiva),
            QtyDaLavorare=_norm_text(ordine.QtyDaLavorare),
            RisorsaAttiva=_norm_text(ordine.RisorsaAttiva),
            LavorazioneAttiva=_norm_text(ordine.LavorazioneAttiva),
            AttrezzaggioAttivo=_norm_text(ordine.AttrezzaggioAttivo),
            RifOrdinePrinc=_norm_text(getattr(ordine, "RifOrdinePrinc", "")),
            Note=ordine.Note,
            FaseConsuntivata=None,
            QuantitaConforme=None,
            QuantitaNonConforme=None,
            TempoFunzionamentoFinale=None,
            TempoNonFunzionamentoMinuti=_norm_text(minuti_non_funzionamento),
            TempoNonFunzionamentoSecondi=_norm_text(secondi_non_funzionamento),
            ChiusuraParziale=None,
            NoteChiusura=" | ".join(note_parts),
            StatoOrdinePre=_norm_text(stato_ordine_pre),
            StatoOrdinePost=_norm_text(stato_ordine_post),
            QtyDaLavorarePre=_norm_text(qty_pre),
            QtyDaLavorarePost=_norm_text(qty_post),
            ClosedBy=_norm_text(suspended_by),
            ClosedAt=_norm_text(suspended_at),
        )
    )


def _add_lotti_usati_logs(
    *,
    operation_group_id: str,
    ordine,
    lotti_input: list[dict],
    fase: str,
    closed_by: str,
    closed_at: str,
):
    for lotto_row in lotti_input or []:
        db.session.add(
            LottiUsatiLog(
                OperationGroupId=operation_group_id,
                IdDocumento=ordine.IdDocumento,
                IdRiga=ordine.IdRiga,
                RifRegistraz=ordine.RifRegistraz,
                CodArt=_norm_text(lotto_row.get("CodArt")),
                RifLottoAlfa=_norm_text(lotto_row.get("RifLottoAlfa")),
                Quantita=str(lotto_row.get("Quantita", 0)),
                Esito=_norm_text(lotto_row.get("Esito", "ok")),
                ClosedBy=_norm_text(closed_by),
                ClosedAt=_norm_text(closed_at),
                Fase=_norm_text(fase),
            )
        )


def _add_lotto_generato_log(
    *,
    operation_group_id: str,
    ordine,
    lotto_prodotto: dict | None,
    closed_by: str,
    closed_at: str,
):
    if lotto_prodotto is None:
        return
    db.session.add(
        LottiGeneratiLog(
            OperationGroupId=operation_group_id,
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
            RifRegistraz=ordine.RifRegistraz,
            CodArt=lotto_prodotto["CodArt"],
            RifLottoAlfa=lotto_prodotto["RifLottoAlfa"],
            Quantita=lotto_prodotto["Quantita"],
            Fase=lotto_prodotto["Fase"],
            ClosedBy=_norm_text(closed_by),
            ClosedAt=_norm_text(closed_at),
        ),
    )


def _build_phase_payload(
    ordine,
    distinta_base,
    fase_corrente: str,
    q_ok: Decimal,
    q_nok: Decimal,
    tempo_finale: str,
    lotti_input: list[dict],
    lotto_prodotto: dict | None,
    note: str,
    now_iso: str,
    chiusura_parziale: bool = False,
    tipo_documento: str = "",
    risorsa: str = "",
    magazzino: str = "",
) -> dict:
    salda_riga = 0 if chiusura_parziale is True else 1
    return {
        "kind": "consuntivo_fase",
        "id_documento": ordine.IdDocumento,
        "id_riga": ordine.IdRiga,
        "rif_registraz": ordine.RifRegistraz,
        "cod_art": ordine.CodArt,
        "descrizione": ordine.DesArt,
        "fase": fase_corrente,
        "quantita_ok": str(q_ok),
        "quantita_ko": str(q_nok),
        "tempo_funzionamento": tempo_finale,
        "note": note,
        "lotti": _normalize_lotti_for_payload(lotti_input),
        "lotto_prodotto": _normalize_lotto_prodotto_for_payload(lotto_prodotto),
        "created_at": now_iso,
        "created_by": _current_username(),
        "salda_riga": salda_riga,
        "tipo_documento": tipo_documento,
        "risorsa": risorsa,
        "magazzino": magazzino,
        "distinta_base": distinta_base,
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
    lines: list[str],
    *,
    prefix: str = "AVPB",
    suffix: str = "",
    encoding: str = "utf-8-sig",
) -> Path:
    path_txt = _build_export_txt_path(prefix=prefix, suffix=suffix)
    content = "\n".join(lines) + "\n"
    path_txt.write_text(content, encoding=encoding, newline="\n")
    return path_txt


def _erp_avp_cfg() -> dict:
    cfg = dict(DEFAULT_AVP_CFG)
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


def _get_pending_avp_export_rows() -> list[dict]:
    rows = []
    for outbox in _get_pending_avp_outbox():
        rows.append(
            {
                "outbox": outbox,
                "payload": _get_outbox_payload(outbox),
                "source_row": _get_export_source_row(outbox),
            }
        )
    return rows


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
    return render_template(
        "home.j2",
        active_partial=template,
        active_tab=tab,
        policy=policy,
        odp=odp,
        causali_attivita=causali,
        bridge_url=url_for("main.api_home_bridge", tab=tab),
        bridge_last_event_id=_last_log_token(),
    )


def _query_for_tab(policy, reparto_code):
    q = _base_odp_query()
    q = policy.filter_input_odp_for_reparto(q, reparto_code)
    return q


def _json_safe(value):
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, set):
        return sorted(_json_safe(v) for v in value)
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


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


def _first_code_from_cell(value) -> str:
    for code in _extract_codes_from_cell(value):
        code = _norm_text(code)
        if code:
            return code
    return ""


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

    after = _norm_text(request.args.get("after"))
    last_event_id = _last_log_token()

    if after and after == last_event_id:
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


def _build_rif_ordine_princ(id_documento: str, id_riga: str) -> str:
    return json.dumps(
        [_norm_text(id_documento), _norm_text(id_riga)],
        ensure_ascii=False,
    )


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


def _apply_stop_minutes_to_runtime(
    stato,
    minuti_non_funzionamento: int,
    *,
    max_removable_seconds: int | None = None,
) -> tuple[int, str]:
    """
    Sottrae i minuti di non funzionamento dal totale Tempo_funzionamento.
    Se max_removable_seconds è valorizzato, limita la sottrazione.
    """
    if stato is None or minuti_non_funzionamento <= 0:
        return 0, _norm_text(getattr(stato, "Tempo_funzionamento", "")) or "0"

    total_seconds = _tempo_to_seconds(stato.Tempo_funzionamento)
    requested_seconds = minuti_non_funzionamento * 60

    removable_seconds = min(requested_seconds, total_seconds)

    if max_removable_seconds is not None:
        removable_seconds = min(removable_seconds, max(0, int(max_removable_seconds)))

    new_total_seconds = max(0, total_seconds - removable_seconds)
    stato.Tempo_funzionamento = _seconds_to_tempo_text(new_total_seconds)

    return removable_seconds, _norm_text(stato.Tempo_funzionamento) or "0"


def _ensure_stato_attivo(
    ordine,
    stato,
    username: str,
    when_dt: datetime,
    fase_corrente: str,
    rif_ordine_princ: str | None = None,
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
            AttrezzaggioAttivo=_norm_text(getattr(ordine, "AttrezzaggioAttivo", "")),
            RifOrdinePrinc=rif_ordine_princ,
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
    if rif_ordine_princ is not None:
        stato.RifOrdinePrinc = rif_ordine_princ

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


def _append_operazione_log(
    *,
    topic: str,
    ordine,
    action: str,
    event_at: str,
    username: str,
    runtime_pre: dict | None,
    runtime_post: dict | None,
    stato_ordine_pre: str = "",
    stato_ordine_post: str = "",
    qty_pre: str = "",
    qty_post: str = "",
    q_ok: str = "",
    q_nok: str = "",
    elapsed_seconds: int | str | None = None,
    tempo_non_funzionamento_minuti: int | str | None = None,
    tempo_non_funzionamento_secondi: int | str | None = None,
    causale: str = "",
    note: str = "",
    motivo: str = "",
    fase: str = "",
    extra_payload: dict | None = None,
):
    runtime_pre = runtime_pre or {}
    runtime_post = runtime_post or {}

    reparto_codes = _extract_codes_from_cell(ordine.CodReparto)
    scope = reparto_codes[0] if reparto_codes else _norm_text(ordine.CodReparto)

    payload = {
        "azione": action,
        "utente": username,
        "fase": _first_not_blank(
            fase,
            _norm_text(runtime_post.get("fase")),
            _norm_text(runtime_pre.get("fase")),
            default="",
        ),
        "tempo_funzionamento": _norm_text(runtime_post.get("tempo_funzionamento")),
    }
    if q_ok not in (None, ""):
        payload["quantita_conforme"] = _norm_text(q_ok)
    if q_nok not in (None, ""):
        payload["quantita_non_conforme"] = _norm_text(q_nok)
    if elapsed_seconds not in (None, ""):
        payload["elapsed_seconds"] = elapsed_seconds
    if tempo_non_funzionamento_minuti not in (None, ""):
        payload["tempo_non_funzionamento_minuti"] = tempo_non_funzionamento_minuti
    if tempo_non_funzionamento_secondi not in (None, ""):
        payload["tempo_non_funzionamento_secondi"] = tempo_non_funzionamento_secondi
    if causale:
        payload["causale"] = causale
    if note:
        payload["note"] = note
    if extra_payload:
        payload.update(extra_payload)

    operation_group_id = _build_operation_group_id(
        ordine=ordine,
        action=action,
        when_iso=event_at,
    )

    row = OdpRuntimeLog(
        OperationGroupId=operation_group_id,
        EventSequence=1,
        Topic=topic,
        Scope=scope,
        CodArt=_norm_text(ordine.CodArt),
        CodReparto=_norm_text(ordine.CodReparto),
        PayloadJson=json.dumps(payload, ensure_ascii=False),
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
        RifRegistraz=ordine.RifRegistraz,
        Azione=action,
        Motivo=_norm_text(motivo),
        UtenteOperazione=username,
        EventAt=event_at,
        StatoOdpPre=_norm_text(runtime_pre.get("stato_odp")),
        StatoOdpPost=_norm_text(runtime_post.get("stato_odp")),
        StatoOrdinePre=_norm_text(stato_ordine_pre),
        StatoOrdinePost=_norm_text(stato_ordine_post),
        FasePre=_norm_text(runtime_pre.get("fase")),
        FasePost=_norm_text(runtime_post.get("fase")),
        DataInCaricoPre=_norm_text(runtime_pre.get("data_in_carico")),
        DataInCaricoPost=_norm_text(runtime_post.get("data_in_carico")),
        DataUltimaAttivazionePre=_norm_text(runtime_pre.get("data_ultima_attivazione")),
        DataUltimaAttivazionePost=_norm_text(
            runtime_post.get("data_ultima_attivazione")
        ),
        TempoFunzionamentoPre=_norm_text(runtime_pre.get("tempo_funzionamento")),
        TempoFunzionamentoPost=_norm_text(runtime_post.get("tempo_funzionamento")),
        ElapsedSeconds=_norm_text(elapsed_seconds),
        TempoNonFunzionamentoMinuti=_norm_text(tempo_non_funzionamento_minuti),
        TempoNonFunzionamentoSecondi=_norm_text(tempo_non_funzionamento_secondi),
        QtyDaLavorarePre=_norm_text(qty_pre),
        QtyDaLavorarePost=_norm_text(qty_post),
        QuantitaConforme=_norm_text(q_ok),
        QuantitaNonConforme=_norm_text(q_nok),
        Causale=_norm_text(causale),
        Note=_norm_text(note),
        RifOrdinePrinc=_first_not_blank(
            runtime_post.get("rif_ordine_princ"),
            runtime_pre.get("rif_ordine_princ"),
            default="",
        ),
    )
    db.session.add(row)
    db.session.flush()
    return row


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
        db.session.flush()

    db.session.delete(ordine)
    db.session.flush()


@main_bp.post("/api/ordini/presa")
@login_required
@require_perm("home")
def api_prendi_ordine():
    data = request.get_json(silent=True) or {}

    id_documento = _norm_text(data.get("id_documento"))
    id_riga = _norm_text(data.get("id_riga"))
    id_documento_principale = _norm_text(data.get("id_documento_principale"))
    id_riga_principale = _norm_text(data.get("id_riga_principale"))

    rif_ordine_princ = None

    if id_documento_principale or id_riga_principale:
        if not id_documento_principale or not id_riga_principale:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Per l'ordine mascherato servono sia id_documento_principale sia id_riga_principale",
                    }
                ),
                400,
            )

        if id_documento_principale == id_documento and id_riga_principale == id_riga:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "L'ordine principale non può coincidere con l'ordine preso in carico",
                    }
                ),
                400,
            )

        rif_ordine_princ = _build_rif_ordine_princ(
            id_documento_principale,
            id_riga_principale,
        )

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
                    "last_event_id": _last_log_token(),
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

        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()
        stato_ordine_pre = _norm_text(ordine.StatoOrdine)
        qty_pre = _qty_da_lavorare_text(ordine)
        now_iso = now_dt.isoformat(timespec="seconds")

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
            rif_ordine_princ=rif_ordine_princ,
        )
        ordine.StatoOrdine = "Attivo"
        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="presa_in_carico",
            when_iso=now_iso,
        )

        _add_input_odp_takeover_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post=_norm_text(ordine.StatoOrdine),
            qty_pre=qty_pre,
            qty_post=_qty_da_lavorare_text(ordine),
            taken_by=_current_username(),
            taken_at=now_iso,
            note_evento="Presa in carico ordine",
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
                "last_event_id": _last_log_token(),
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

    tempo_non_funzionamento_raw = data.get("tempo_non_funzionamento_minuti")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_fermo_macchina")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_macchina_ferma")

    try:
        minuti_non_funzionamento = _parse_minuti_non_funzionamento(
            tempo_non_funzionamento_raw
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

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

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()
        stato_ordine_pre = _norm_text(ordine.StatoOrdine)
        qty_pre = _qty_da_lavorare_text(ordine)
        now_iso = now_dt.isoformat(timespec="seconds")

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

        removed_seconds, tempo_funzionamento = _apply_stop_minutes_to_runtime(
            stato,
            minuti_non_funzionamento,
        )

        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="sospensione",
            when_iso=now_iso,
        )

        _add_input_odp_suspend_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post="In Sospeso",
            qty_pre=qty_pre,
            qty_post=_norm_text(stato.QtyDaLavorare),
            suspended_by=_current_username(),
            suspended_at=now_iso,
            causale=causale,
            minuti_non_funzionamento=minuti_non_funzionamento,
            secondi_non_funzionamento=removed_seconds,
            note_evento="Sospensione ordine",
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
                "last_event_id": _last_log_token(),
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

        stato = InputOdpRuntime.query.filter_by(
            IdDocumento=ordine.IdDocumento,
            IdRiga=ordine.IdRiga,
        ).first()
        stato_ordine_pre = _norm_text(ordine.StatoOrdine)
        qty_pre = _qty_da_lavorare_text(ordine)
        now_iso = now_dt.isoformat(timespec="seconds")

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
        tempo_funzionamento = _norm_text(stato.Tempo_funzionamento) or "0"

        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="sospensione",
            when_iso=now_iso,
        )

        _add_input_odp_suspend_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post="In Sospeso",
            qty_pre=qty_pre,
            qty_post=_norm_text(stato.QtyDaLavorare),
            suspended_by=_current_username(),
            suspended_at=now_iso,
            causale=causale,
            minuti_non_funzionamento=None,
            secondi_non_funzionamento=None,
            note_evento="Sospensione ordine",
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
                "last_event_id": _last_log_token(),
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
                    "last_event_id": _last_log_token(),
                    "fragments": fragments,
                }
            ),
            409,
        )

    stato_attuale = _norm_text(ordine.StatoOrdine)
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
    ).first()

    stato_attuale = _norm_text(getattr(stato, "Stato_odp", "")) or _norm_text(
        ordine.StatoOrdine
    )
    stato_norm = stato_attuale.lower()
    changed = False
    message = None

    if stato_norm == "in sospeso":
        now_dt = _now_rome_dt()

        _sync_active_fields_for_phase(ordine, fase_corrente)

        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine. "
                            "La riattivazione non può ripristinare correttamente il runtime."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato_ordine_pre = _norm_text(stato_attuale)
        qty_pre = _qty_da_lavorare_text(ordine, stato=stato)
        now_iso = now_dt.isoformat(timespec="seconds")
        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="riattivazione",
            when_iso=now_iso,
        )

        _add_input_odp_takeover_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post="Attivo",
            qty_pre=qty_pre,
            qty_post=_norm_text(stato.QtyDaLavorare),
            taken_by=_current_username(),
            taken_at=now_iso,
            note_evento="Riattivazione ordine",
        )
        _sync_active_fields_for_phase(ordine, fase_corrente)
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
                "last_event_id": _last_log_token(),
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
                    "last_event_id": _last_log_token(),
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
                            "La riattivazione non può ripristinare correttamente il runtime."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        _sync_active_fields_for_phase(ordine, fase_corrente)

        stato_ordine_pre = _norm_text(stato_attuale)
        qty_pre = _qty_da_lavorare_text(ordine)
        now_iso = now_dt.isoformat(timespec="seconds")

        stato = _ensure_stato_attivo(
            ordine=ordine,
            stato=stato,
            username=_current_username(),
            when_dt=now_dt,
            fase_corrente=fase_corrente,
        )

        operation_group_id = _build_operation_group_id(
            ordine=ordine,
            action="riattivazione_macchina",
            when_iso=now_iso,
        )

        _add_input_odp_takeover_log(
            operation_group_id=operation_group_id,
            ordine=ordine,
            stato_ordine_pre=stato_ordine_pre,
            stato_ordine_post="Attivo",
            qty_pre=qty_pre,
            qty_post=_norm_text(stato.QtyDaLavorare),
            taken_by=_current_username(),
            taken_at=now_iso,
            note_evento=f"Riattivazione ordine macchina | Matricola: {matricola}",
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
                "last_event_id": _last_log_token(),
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

    tempo_non_funzionamento_raw = data.get("tempo_non_funzionamento_minuti")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_fermo_macchina")
    if tempo_non_funzionamento_raw is None:
        tempo_non_funzionamento_raw = data.get("tempo_macchina_ferma")

    try:
        minuti_non_funzionamento = _parse_minuti_non_funzionamento(
            tempo_non_funzionamento_raw
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

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
    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
    ).first()
    try:
        q_tot = _qty_da_lavorare_decimal(ordine, stato=stato)
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
    qty_pre_text = _qty_da_lavorare_text(ordine, stato=stato)

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
    action_name = "chiusura_parziale" if chiusura_parziale else "chiusura_finale"
    operation_group_id = _build_operation_group_id(
        ordine=ordine,
        action=action_name,
        when_iso=now_iso,
    )

    fase_corrente = _fase_corrente_for_export(ordine, stato=stato)

    if _norm_text(ordine.GestioneLotto).lower() == "si" and q_ok > 0:
        rif_lotto_prodotto = generazione_lotti(now_dt)

        for row in lotti_input:
            esito_row = _norm_text(row.get("Esito", "ok")).lower()
            if esito_row != "ok":
                continue

        lotto_prodotto = {
            "CodArt": ordine.CodArt,
            "RifLottoAlfa": rif_lotto_prodotto,
            "Quantita": _decimal_to_text(q_ok),
            "Fase": fase_corrente,
        }

    tempo_finale = "0"
    elapsed_seconds = 0
    removed_seconds = 0
    runtime_pre = _runtime_snapshot(stato)
    stato_ordine_pre = _norm_text(ordine.StatoOrdine)
    qty_pre = _qty_da_lavorare_text(ordine)

    if stato is not None:
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            elapsed_seconds = _accumulate_runtime_until(stato, now_dt)

        removed_seconds, tempo_finale = _apply_stop_minutes_to_runtime(
            stato,
            minuti_non_funzionamento,
        )

    outbox = None

    if chiusura_parziale:
        if stato is None:
            db.session.rollback()
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": (
                            "Record runtime non trovato per questo ordine. "
                            "La chiusura non può proseguire in modo coerente."
                        ),
                        "id_documento": ordine.IdDocumento,
                        "id_riga": ordine.IdRiga,
                    }
                ),
                409,
            )

        payload = _build_phase_payload(
            distinta_base=ordine.DistintaMateriale,
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
            tipo_documento=ordine.CodTipoDoc,
            risorsa=ordine.RisorsaAttiva,
            magazzino=ordine.CodMagPrincipale,
        )
        outbox = _queue_phase_export(
            ordine=ordine,
            fase_corrente=fase_corrente,
            payload=payload,
        )
    else:
        payload = _build_phase_payload(
            ordine=ordine,
            distinta_base=ordine.DistintaMateriale,
            fase_corrente=fase_corrente,
            q_ok=q_ok,
            q_nok=q_nok,
            tempo_finale=tempo_finale,
            lotti_input=lotti_input,
            lotto_prodotto=lotto_prodotto,
            note=note,
            now_iso=now_iso,
            chiusura_parziale=False,
            tipo_documento=ordine.CodTipoDoc,
            risorsa=ordine.RisorsaAttiva,
            magazzino=ordine.CodMagPrincipale,
        )
        outbox = _queue_phase_export(
            ordine=ordine,
            fase_corrente=fase_corrente,
            payload=payload,
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
    runtime_post = _runtime_snapshot(stato)

    if transition["tipo"] == "finale" and stato is not None:
        runtime_post["stato_odp"] = "Chiusa"
        runtime_post["data_ultima_attivazione"] = ""

    if chiusura_parziale:
        stato_post_log = "In Sospeso"
        qty_post_log = qty_residua_text
    elif transition["tipo"] == "finale":
        stato_post_log = "Chiusa"
        qty_post_log = "0"

    note_chiusura_log = note
    stato_post_log = _norm_text(ordine.StatoOrdine)
    qty_post_log = _norm_text(ordine.QtyDaLavorare)

    if chiusura_parziale:
        stato_post_log = "In Sospeso"
        qty_post_log = qty_residua_text
    elif transition["tipo"] == "finale":
        stato_post_log = "Chiusa"
        qty_post_log = "0"
    if chiusura_parziale:
        note_chiusura_log = (
            f"[PARZIALE] residuo={qty_residua_text}; {note}".strip().rstrip(";")
        )
    _add_input_odp_closure_log(
        operation_group_id=operation_group_id,
        ordine=ordine,
        fase_consuntivata=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        tempo_finale=tempo_finale,
        minuti_non_funzionamento=minuti_non_funzionamento,
        secondi_non_funzionamento=removed_seconds,
        chiusura_parziale=chiusura_parziale,
        note_chiusura=note_chiusura_log,
        stato_ordine_pre=stato_ordine_pre,
        stato_ordine_post=stato_post_log,
        qty_pre=qty_pre_text,
        qty_post=qty_post_log,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    _append_operazione_log(
        topic="fase_consuntivata_parziale"
        if chiusura_parziale
        else "fase_consuntivata",
        ordine=ordine,
        action=action_name,
        event_at=now_iso,
        username=_current_username(),
        runtime_pre=runtime_pre,
        runtime_post=runtime_post,
        stato_ordine_pre=stato_ordine_pre,
        stato_ordine_post=stato_post_log,
        qty_pre=qty_pre_text,
        qty_post=qty_post_log,
        q_ok=str(q_ok),
        q_nok=str(q_nok),
        elapsed_seconds=elapsed_seconds,
        tempo_non_funzionamento_minuti=minuti_non_funzionamento,
        tempo_non_funzionamento_secondi=removed_seconds,
        note=note_chiusura_log,
        fase=fase_corrente,
        extra_payload={
            "quantita_lavorata_step": qty_lavorata_text,
            "qty_da_lavorare_pre": qty_pre_text,
            "qty_da_lavorare_post": qty_post_log,
            "lotti_count": len(lotti_input),
            "chiusura_parziale": chiusura_parziale,
            "outbox_id": outbox.outbox_id if outbox else None,
            "export_status": outbox.status if outbox else None,
            "lotto_prodotto": lotto_prodotto,
        },
    )

    _add_lotti_usati_logs(
        operation_group_id=operation_group_id,
        ordine=ordine,
        lotti_input=lotti_input,
        fase=fase_corrente,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    _add_lotto_generato_log(
        operation_group_id=operation_group_id,
        ordine=ordine,
        lotto_prodotto=lotto_prodotto,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    tab = _tab_from_ordine(ordine)
    stato_ordine_response = _norm_text(ordine.StatoOrdine)
    qty_da_lavorare_response = _norm_text(ordine.QtyDaLavorare)

    if chiusura_parziale:
        stato_ordine_response = "In Sospeso"
        qty_da_lavorare_response = qty_residua_text
    elif transition["tipo"] == "finale":
        _delete_closed_order_from_runtime_db(ordine=ordine, stato=stato)
        stato_ordine_response = "Chiusa"
        qty_da_lavorare_response = "0"
    fragments = {}
    if tab:
        reparto_code = BRIDGE_CONFIG[tab]["reparto"]
        odp = list(_query_for_tab(policy, reparto_code).all())
        fragments = RENDERERS[tab](odp)

    des_art_for_label = ordine.DesArt
    quantita_for_label = ordine.Quantita

    db.session.commit()
    if lotto_prodotto is not None:
        gen_etichette(
            str(lotto_prodotto["CodArt"]),
            des_art_for_label,
            str(lotto_prodotto["RifLottoAlfa"]),
            quantita_for_label,
            current_app.config["DIMENSIONI"],
            current_app.config["DPI"],
            current_app.config["FONT_PATH"],
        )
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
                "last_event_id": _last_log_token(),
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
    stato = InputOdpRuntime.query.filter_by(
        IdDocumento=ordine.IdDocumento,
        IdRiga=ordine.IdRiga,
    ).first()

    componenti_richiesti_lotto = _componenti_lotto_per_ordine(
        ordine,
        include_senza_lotti=True,
        ignore_parent_gestione_lotto=True,
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
        q_tot = _qty_da_lavorare_decimal(ordine, stato=stato)
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
    lotto_prodotto = None
    action_name = "chiusura_macchina"
    elapsed_seconds = 0
    minuti_non_funzionamento = 0
    removed_seconds = 0
    runtime_pre = _runtime_snapshot(stato)
    stato_ordine_pre = _norm_text(ordine.StatoOrdine)
    qty_pre = _qty_da_lavorare_text(ordine, stato=stato)
    operation_group_id = _build_operation_group_id(
        ordine=ordine,
        action="chiusura_macchina",
        when_iso=now_iso,
    )

    tempo_finale = "0"
    if stato is not None:
        if _norm_text(stato.Stato_odp).lower().startswith("attiv"):
            elapsed_seconds = _accumulate_runtime_until(stato, now_dt)
        tempo_finale = _norm_text(stato.Tempo_funzionamento) or "0"

    fase_corrente = _fase_corrente_for_export(ordine, stato=stato, fase_override=fase)
    payload = _build_phase_payload(
        ordine=ordine,
        distinta_base=ordine.DistintaMateriale,
        fase_corrente=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        tempo_finale=tempo_finale,
        lotti_input=lotti_input,
        lotto_prodotto=None,
        note=note,
        now_iso=now_iso,
        tipo_documento=ordine.CodTipoDoc,
        risorsa=ordine.RisorsaAttiva,
        magazzino=ordine.CodMagPrincipale,
    )

    outbox = _queue_phase_export(
        ordine=ordine,
        fase_corrente=fase_corrente,
        payload=payload,
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

    runtime_post = _runtime_snapshot(stato)

    if transition["tipo"] == "finale" and stato is not None:
        runtime_post["stato_odp"] = "Chiusa"
        runtime_post["data_ultima_attivazione"] = ""

    note_chiusura_log = note
    if chiusura_parziale:
        note_chiusura_log = (
            f"[PARZIALE] residuo={qty_residua_text}; {note}".strip().rstrip(";")
        )
    _append_operazione_log(
        topic="fase_consuntivata_montaggio_macchina",
        ordine=ordine,
        action=action_name,
        event_at=now_iso,
        username=_current_username(),
        runtime_pre=runtime_pre,
        runtime_post=runtime_post,
        stato_ordine_pre=stato_ordine_pre,
        stato_ordine_post=_norm_text(ordine.StatoOrdine),
        qty_pre=qty_pre,
        qty_post=_norm_text(ordine.QtyDaLavorare),
        q_ok=str(q_ok),
        q_nok=str(q_nok),
        elapsed_seconds=elapsed_seconds,
        tempo_non_funzionamento_minuti=0,
        tempo_non_funzionamento_secondi=0,
        note=note_chiusura_log,
        fase=fase_corrente,
        extra_payload={
            "matricola": matricola,
            "lotti_count": len(lotti_input),
            "outbox_id": outbox.outbox_id if outbox else None,
            "export_status": outbox.status if outbox else None,
        },
    )

    _add_input_odp_closure_log(
        operation_group_id=operation_group_id,
        ordine=ordine,
        fase_consuntivata=fase_corrente,
        q_ok=q_ok,
        q_nok=q_nok,
        tempo_finale=tempo_finale,
        minuti_non_funzionamento=minuti_non_funzionamento,
        secondi_non_funzionamento=removed_seconds,
        chiusura_parziale=chiusura_parziale,
        note_chiusura=note_chiusura_log,
        stato_ordine_pre=stato_ordine_pre,
        stato_ordine_post=_norm_text(ordine.StatoOrdine),
        qty_pre=qty_pre,
        qty_post=_norm_text(ordine.QtyDaLavorare),
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    _add_lotti_usati_logs(
        operation_group_id=operation_group_id,
        ordine=ordine,
        lotti_input=lotti_input,
        fase=fase_corrente,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    _add_lotto_generato_log(
        operation_group_id=operation_group_id,
        ordine=ordine,
        lotto_prodotto=lotto_prodotto,
        closed_by=_current_username(),
        closed_at=now_iso,
    )

    tab = _tab_from_ordine(ordine)
    stato_ordine_response = ordine.StatoOrdine
    qty_da_lavorare_response = _norm_text(ordine.QtyDaLavorare)
    if transition["tipo"] == "finale":
        _delete_closed_order_from_runtime_db(ordine=ordine, stato=stato)
        stato_ordine_response = "Chiusa"
        qty_da_lavorare_response = "0"
        message = (
            "Ordine macchina chiuso definitivamente, archiviato nel db_log "
            "e rimosso dal database operativo."
        )
    else:
        message = (
            f"Fase macchina {transition['fase_corrente']} consuntivata. "
            f"File TXT generato in coda export. "
            f"Ordine mantenuto a DB e riportato in pianificata sulla fase "
            f"{transition['fase_successiva']}."
        )
    fragments = {}
    if tab:
        reparto_code = BRIDGE_CONFIG[tab]["reparto"]
        odp = list(_query_for_tab(policy, reparto_code).all())
        fragments = RENDERERS[tab](odp)

    db.session.commit()

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
                "last_event_id": _last_log_token(),
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

    export_rows = _get_pending_avp_export_rows()
    if not export_rows:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Nessun record ERP pending da esportare",
                }
            ),
            404,
        )

    outbox_rows = [row["outbox"] for row in export_rows]
    try:
        list_line = txt_generator(
            export_rows,
            cfg=_erp_avp_cfg(),
        )
        path_txt = _write_txt_content(
            list_line,
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


@main_bp.route("/impostazioni")
@login_required
def impostazioni():
    if not current_user.has_permission("impostazioni_utente"):
        abort(403)

    policy = RbacPolicy(current_user)

    show_role_assignment_section = policy.can_view_role_assignment_section
    show_user_abac_section = policy.can_view_user_abac_section

    ruolo_options = []
    utenti_per_ruolo = {}
    ruolo_details = {}
    user_abac_details = {}

    manageable_users = []
    manageable_roles = []

    show_role_links_section = policy.can_view_role_links_section

    role_link_tables = []
    role_link_details = {}
    role_link_role_options = []

    permission_role_options = []
    permission_details = {}

    ruoli_link_gestibili = policy.role_link_manageable_roles()

    if show_role_assignment_section:
        assignable_users = (
            policy.role_assignment_users_query().order_by(User.username.asc()).all()
        )

        assignable_roles = (
            policy.role_assignment_roles_query()
            .order_by(
                func.lower(func.coalesce(Roles.description, Roles.name)),
                func.lower(Roles.name),
            )
            .all()
        )

        manageable_users = []
        for utente in assignable_users:
            ruolo_corrente = utente.roles[0] if utente.roles else None

            manageable_users.append(
                {
                    "id": utente.id,
                    "username": utente.username or "",
                    "current_role_id": ruolo_corrente.id if ruolo_corrente else None,
                    "current_role_name": ruolo_corrente.name if ruolo_corrente else "",
                    "current_role_description": (
                        ruolo_corrente.description or ruolo_corrente.name
                    )
                    if ruolo_corrente
                    else "",
                }
            )

        manageable_roles = [
            {
                "id": ruolo.id,
                "name": ruolo.name or "",
                "description": ruolo.description or ruolo.name or "",
            }
            for ruolo in assignable_roles
        ]

    if show_role_links_section:
        role_link_role_options = ruoli_link_gestibili

        role_link_tables = [
            {"key": key, "label": cfg["label"]} for key, cfg in ROLE_LINK_CONFIG.items()
        ]

        for ruolo in ruoli_link_gestibili:
            role_link_details[str(ruolo.id)] = {
                "id": ruolo.id,
                "name": ruolo.name or "",
                "description": ruolo.description or "",
                "tables": {},
            }

            for key, cfg in ROLE_LINK_CONFIG.items():
                model = cfg["model"]
                code_attr = cfg["code_attr"]
                desc_attr = cfg["desc_attr"]

                if model is Roles:
                    all_items = [
                        item
                        for item in policy.role_link_manageable_roles()
                        if int(item.id) != int(ruolo.id)
                    ]
                else:
                    all_items = model.query.order_by(
                        func.lower(
                            func.coalesce(
                                getattr(model, desc_attr),
                                getattr(model, code_attr),
                            )
                        ),
                        func.lower(getattr(model, code_attr)),
                    ).all()

                selected_ids = set()
                assoc_table = cfg["assoc_table"]
                left_col = getattr(assoc_table.c, cfg["left_fk"])
                right_col = getattr(assoc_table.c, cfg["right_fk"])

                stmt = select(right_col).where(left_col == ruolo.id)
                selected_ids = set(db.session.execute(stmt).scalars().all())

                role_link_details[str(ruolo.id)]["tables"][key] = {
                    "label": cfg["label"],
                    "items": [
                        {
                            "id": getattr(item, cfg["model_id"]),
                            "codice": getattr(item, code_attr, "") or "",
                            "descrizione": getattr(item, desc_attr, "") or "",
                            "checked": getattr(item, cfg["model_id"]) in selected_ids,
                        }
                        for item in all_items
                    ],
                }

    if show_user_abac_section:
        ruoli_gestibili = policy.abac_manageable_roles()

        for ruolo in ruoli_gestibili:
            utenti_ruolo = (
                ruolo.users.filter(User.active.is_(True))
                .order_by(User.username.asc())
                .all()
            )

            if not utenti_ruolo:
                continue

            ruolo_options.append(ruolo)
            utenti_per_ruolo[ruolo.id] = utenti_ruolo

            lavorazioni = sorted(
                ruolo.effective_lavorazioni,
                key=lambda x: ((x.Codice or "").lower(), (x.Descrizione or "").lower()),
            )
            risorse = sorted(
                ruolo.effective_risorse,
                key=lambda x: ((x.Codice or "").lower(), (x.Descrizione or "").lower()),
            )

            ruolo_lavorazioni_ids = {x.id for x in lavorazioni}
            ruolo_risorse_ids = {x.id for x in risorse}

            ruolo_details[str(ruolo.id)] = {
                "id": ruolo.id,
                "name": ruolo.name or "",
                "description": ruolo.description or "",
                "lavorazioni": [
                    {
                        "id": x.id,
                        "codice": x.Codice or "",
                        "descrizione": x.Descrizione or "",
                    }
                    for x in lavorazioni
                ],
                "risorse": [
                    {
                        "id": x.id,
                        "codice": x.Codice or "",
                        "descrizione": x.Descrizione or "",
                    }
                    for x in risorse
                ],
            }

            user_abac_details[str(ruolo.id)] = {}

            for utente in utenti_ruolo:
                user_abac_details[str(ruolo.id)][str(utente.id)] = {
                    "id": utente.id,
                    "username": utente.username or "",
                    "lavorazioni_ids": sorted(
                        x.id
                        for x in (utente.lavorazioni or [])
                        if x.id in ruolo_lavorazioni_ids
                    ),
                    "risorse_ids": sorted(
                        x.id
                        for x in (utente.risorse or [])
                        if x.id in ruolo_risorse_ids
                    ),
                }

    return render_template(
        "impostazioni.j2",
        ruolo_options=ruolo_options,
        utenti_per_ruolo=utenti_per_ruolo,
        ruolo_details=ruolo_details,
        user_abac_details=user_abac_details,
        manageable_users=manageable_users,
        manageable_roles=manageable_roles,
        show_role_assignment_section=show_role_assignment_section,
        show_user_abac_section=show_user_abac_section,
        permission_role_options=permission_role_options,
        permission_details=permission_details,
        role_link_tables=role_link_tables,
        role_link_details=role_link_details,
        show_role_links_section=show_role_links_section,
        role_link_role_options=role_link_role_options,
    )


@main_bp.post("/api/impostazioni/assegna-ruolo")
@login_required
def api_assegna_ruolo():
    policy = RbacPolicy(current_user)

    if not policy.can_view_role_assignment_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    user_id_raw = data.get("user_id")
    role_id_raw = data.get("role_id")

    try:
        user_id = int(user_id_raw)
        role_id = int(role_id_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Parametri non validi."}), 400

    utente = User.query.get(user_id)
    if utente is None:
        return jsonify({"ok": False, "error": "Utente non trovato."}), 404

    ruolo = Roles.query.get(role_id)
    if ruolo is None:
        return jsonify({"ok": False, "error": "Ruolo non trovato."}), 404

    if not policy.can_manage_target_user(utente):
        return jsonify({"ok": False, "error": "Utente non gestibile."}), 403

    if not policy.can_assign_target_role(ruolo):
        return jsonify({"ok": False, "error": "Ruolo non assegnabile."}), 403

    try:
        db.session.execute(delete(user_roles).where(user_roles.c.user_id == utente.id))

        db.session.execute(
            user_roles.insert().values(
                user_id=utente.id,
                role_id=ruolo.id,
            )
        )
        db.session.execute(
            delete(users_lavorazioni).where(users_lavorazioni.c.user_id == utente.id)
        )

        db.session.execute(
            delete(users_risorse).where(users_risorse.c.user_id == utente.id)
        )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore assegnazione ruolo: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Ruolo assegnato correttamente.",
            "user_id": utente.id,
            "role_id": ruolo.id,
            "role_name": ruolo.name or "",
            "role_description": ruolo.description or ruolo.name or "",
        }
    ), 200


@main_bp.post("/api/impostazioni/utente-abac")
@login_required
def api_save_user_abac():
    policy = RbacPolicy(current_user)
    if not current_user.has_permission("impostazioni_utente"):
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    role_id_raw = data.get("role_id")
    user_id_raw = data.get("user_id")
    lavorazioni_ids_raw = data.get("lavorazioni_ids") or []
    risorse_ids_raw = data.get("risorse_ids") or []

    try:
        role_id = int(role_id_raw)
        user_id = int(user_id_raw)
        lavorazioni_ids = {int(x) for x in lavorazioni_ids_raw}
        risorse_ids = {int(x) for x in risorse_ids_raw}
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Parametri non validi."}), 400

    ruolo = Roles.query.get(role_id)
    if ruolo is None:
        return jsonify({"ok": False, "error": "Ruolo non trovato."}), 404

    utente = User.query.get(user_id)
    if utente is None:
        return jsonify({"ok": False, "error": "Utente non trovato."}), 404

    if not any(r.id == ruolo.id for r in (utente.roles or [])):
        return jsonify(
            {"ok": False, "error": "L'utente non appartiene al ruolo selezionato."}
        ), 400

    allowed_lavorazioni_ids = {x.id for x in ruolo.effective_lavorazioni}
    allowed_risorse_ids = {x.id for x in ruolo.effective_risorse}

    # Protezione: niente estensioni oltre RBAC
    invalid_lavorazioni = lavorazioni_ids - allowed_lavorazioni_ids
    invalid_risorse = risorse_ids - allowed_risorse_ids

    if invalid_lavorazioni or invalid_risorse:
        return jsonify(
            {
                "ok": False,
                "error": "Il payload contiene assegnazioni fuori dal perimetro RBAC del ruolo.",
                "invalid_lavorazioni": sorted(invalid_lavorazioni),
                "invalid_risorse": sorted(invalid_risorse),
            }
        ), 400

    # Stato attuale utente globale
    current_lavorazioni_ids = {x.id for x in (utente.lavorazioni or [])}
    current_risorse_ids = {x.id for x in (utente.risorse or [])}

    # Lavora SOLO nel perimetro del ruolo selezionato
    current_lavorazioni_in_scope = current_lavorazioni_ids & allowed_lavorazioni_ids
    current_risorse_in_scope = current_risorse_ids & allowed_risorse_ids

    lavorazioni_to_add = lavorazioni_ids - current_lavorazioni_in_scope
    lavorazioni_to_remove = current_lavorazioni_in_scope - lavorazioni_ids

    risorse_to_add = risorse_ids - current_risorse_in_scope
    risorse_to_remove = current_risorse_in_scope - risorse_ids

    try:
        if lavorazioni_to_add:
            db.session.execute(
                users_lavorazioni.insert(),
                [
                    {"user_id": utente.id, "lavorazioni_id": item_id}
                    for item_id in sorted(lavorazioni_to_add)
                ],
            )

        if lavorazioni_to_remove:
            db.session.execute(
                delete(users_lavorazioni).where(
                    users_lavorazioni.c.user_id == utente.id,
                    users_lavorazioni.c.lavorazioni_id.in_(
                        sorted(lavorazioni_to_remove)
                    ),
                )
            )

        if risorse_to_add:
            db.session.execute(
                users_risorse.insert(),
                [
                    {"user_id": utente.id, "risorse_id": item_id}
                    for item_id in sorted(risorse_to_add)
                ],
            )

        if risorse_to_remove:
            db.session.execute(
                delete(users_risorse).where(
                    users_risorse.c.user_id == utente.id,
                    users_risorse.c.risorse_id.in_(sorted(risorse_to_remove)),
                )
            )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore salvataggio impostazioni ABAC: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Impostazioni ABAC salvate correttamente.",
            "role_id": ruolo.id,
            "user_id": utente.id,
            "lavorazioni_ids": sorted(lavorazioni_ids),
            "risorse_ids": sorted(risorse_ids),
            "delta": {
                "lavorazioni": {
                    "added": sorted(lavorazioni_to_add),
                    "removed": sorted(lavorazioni_to_remove),
                },
                "risorse": {
                    "added": sorted(risorse_to_add),
                    "removed": sorted(risorse_to_remove),
                },
            },
        }
    ), 200


@main_bp.post("/api/impostazioni/ruolo-link")
@login_required
def api_save_role_links():
    policy = RbacPolicy(current_user)

    if not policy.can_view_role_links_section:
        return jsonify({"ok": False, "error": "Permesso insufficiente."}), 403

    data = request.get_json(silent=True) or {}

    role_id_raw = data.get("role_id")
    table_key = (data.get("table_key") or "").strip()
    selected_ids_raw = data.get("selected_ids") or []

    try:
        role_id = int(role_id_raw)
        selected_ids = {int(x) for x in selected_ids_raw}
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Parametri non validi."}), 400

    if table_key not in ROLE_LINK_CONFIG:
        return jsonify({"ok": False, "error": "Tabella non valida."}), 400

    ruolo = Roles.query.get(role_id)
    if ruolo is None:
        return jsonify({"ok": False, "error": "Ruolo non trovato."}), 404
    cfg = ROLE_LINK_CONFIG[table_key]
    assoc_table = cfg["assoc_table"]
    model = cfg["model"]

    if cfg["model"] is Roles:
        allowed_role_ids = {int(r.id) for r in policy.role_link_manageable_roles()}

        allowed_role_ids.discard(int(ruolo.id))

        invalid_role_ids = selected_ids - allowed_role_ids
        if invalid_role_ids:
            return jsonify(
                {
                    "ok": False,
                    "error": "Il payload contiene ruoli non consentiti o di livello uguale/superiore.",
                    "invalid_ids": sorted(invalid_role_ids),
                }
            ), 400

    if not policy.can_manage_target_role(ruolo):
        return jsonify({"ok": False, "error": "Ruolo non gestibile."}), 403

    if table_key in {"ruoli_ereditati", "ruoli_gestibili"}:
        manageable_role_ids = {r.id for r in policy.role_link_manageable_roles()}

        if role_id in selected_ids:
            return jsonify(
                {"ok": False, "error": "Un ruolo non può essere collegato a sé stesso."}
            ), 400

        invalid_target_ids = selected_ids - manageable_role_ids
        if invalid_target_ids:
            return jsonify(
                {
                    "ok": False,
                    "error": "Il payload contiene ruoli non gestibili.",
                    "invalid_ids": sorted(invalid_target_ids),
                }
            ), 400

    valid_ids = {getattr(item, cfg["model_id"]) for item in model.query.all()}

    invalid_ids = selected_ids - valid_ids
    if invalid_ids:
        return jsonify(
            {
                "ok": False,
                "error": "Il payload contiene id non validi.",
                "invalid_ids": sorted(invalid_ids),
            }
        ), 400

    left_col = getattr(assoc_table.c, cfg["left_fk"])
    right_col = getattr(assoc_table.c, cfg["right_fk"])

    current_ids = set(
        db.session.execute(select(right_col).where(left_col == ruolo.id))
        .scalars()
        .all()
    )

    to_add = selected_ids - current_ids
    to_remove = current_ids - selected_ids

    try:
        if to_add:
            db.session.execute(
                assoc_table.insert(),
                [
                    {
                        cfg["left_fk"]: ruolo.id,
                        cfg["right_fk"]: item_id,
                    }
                    for item_id in sorted(to_add)
                ],
            )

        if to_remove:
            db.session.execute(
                delete(assoc_table).where(
                    left_col == ruolo.id,
                    right_col.in_(sorted(to_remove)),
                )
            )

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify(
            {
                "ok": False,
                "error": f"Errore salvataggio connessioni ruolo: {exc}",
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": "Connessioni ruolo salvate correttamente.",
            "role_id": ruolo.id,
            "table_key": table_key,
            "selected_ids": sorted(selected_ids),
            "delta": {
                "added": sorted(to_add),
                "removed": sorted(to_remove),
            },
        }
    ), 200


def _hours_to_work_days(total_hours: float, hours_per_day: float = 8.0) -> float:
    total = float(total_hours or 0.0)
    if total <= 0:
        return 0.0
    return round(total / hours_per_day, 2)


@main_bp.get("/api/dash-complessiva")
@main_bp.get("/dash-complessiva")
@login_required
@require_perm("dash_complessiva")
def dash_complessiva():
    policy = RbacPolicy(current_user)
    ordini_visibili = policy.filter_input_odp(_base_odp_query()).all()
    ordini_globali_unici = set()

    allowed_reparti = {
        _norm_text(code)
        for code in getattr(policy, "allowed_reparti", [])
        if _norm_text(code)
    }

    reparti_rows = []
    if allowed_reparti:
        reparti_rows = (
            Reparti.query.filter(Reparti.Codice.in_(sorted(allowed_reparti)))
            .order_by(Reparti.Descrizione.asc(), Reparti.Codice.asc())
            .all()
        )

    kpi_globali = _dash_complessiva_new_bucket()

    kpi_per_reparto = {}
    for rep in reparti_rows:
        codice = _norm_text(rep.Codice)
        if not codice:
            continue

        kpi_per_reparto[codice] = {
            "codice": codice,
            "descrizione": _first_not_blank(rep.Descrizione, rep.Codice, default="-"),
            "kpi": _dash_complessiva_new_bucket(),
            "_seen_order_keys": set(),
        }

    for ordine in ordini_visibili:
        chiave_ordine = (ordine.IdDocumento, ordine.IdRiga)
        ordini_globali_unici.add(chiave_ordine)
        stato = _norm_text(getattr(ordine, "StatoOrdine", "")).lower()
        if stato == "chiusa":
            continue

        # Nel tuo caso questi valori vanno interpretati come minuti correnti
        ore_lavorazione, ore_attrezzaggio = _order_hours_snapshot_complessiva(ordine)

        _dash_complessiva_apply_order(
            kpi_globali,
            stato,
            ore_lavorazione,
            ore_attrezzaggio,
        )

        fase_attiva = _norm_text(getattr(ordine, "FaseAttiva", ""))

        reparto_attivo_raw = _active_value_for_phase(
            getattr(ordine, "CodReparto", ""),
            getattr(ordine, "NumFase", ""),
            fase_attiva,
        )
        reparto_attivo = _first_code_from_cell(reparto_attivo_raw)

        if reparto_attivo in kpi_per_reparto:
            _dash_complessiva_apply_order(
                kpi_per_reparto[reparto_attivo]["kpi"],
                stato,
                ore_lavorazione,
                ore_attrezzaggio,
            )

    kpi_globali = _dash_complessiva_finalize_bucket(kpi_globali)

    kpi_reparti = []
    for reparto_code in sorted(
        kpi_per_reparto,
        key=lambda code: (
            (kpi_per_reparto[code]["descrizione"] or "").lower(),
            code.lower(),
        ),
    ):
        payload = kpi_per_reparto[reparto_code]
        payload["kpi"] = _dash_complessiva_finalize_bucket(payload["kpi"])
        kpi_reparti.append(payload)

    return render_template(
        "dash_complessiva.j2",
        kpi_globali=_json_safe(kpi_globali),
        kpi_reparti=_json_safe(kpi_reparti),
    )


@main_bp.get("/api/dash-reparto")
@main_bp.get("/dash-reparto")
@login_required
@require_perm("dash_reparto")
def dash_reparto():
    manageable_role_ids = current_user.manageable_role_ids
    utenti_subordinati = []

    if manageable_role_ids:
        utenti_subordinati = (
            User.query.join(user_roles, user_roles.c.user_id == User.id)
            .filter(
                User.active.is_(True),
                User.id != current_user.id,
                user_roles.c.role_id.in_(manageable_role_ids),
            )
            .distinct()
            .order_by(User.username.asc())
            .all()
        )

    utenti_data = {}
    utenti_data[current_user.username] = {
        "id": current_user.id,
        "username": current_user.username,
        "is_current": True,
        "kpi": {
            "attivi": 0,
            "sospesi": 0,
            "ore_lavorazione_attivi": 0.0,
            "ore_lavorazione_sospesi": 0.0,
            "minuti_attrezzaggio_attivi": 0.0,
            "minuti_attrezzaggio_sospesi": 0.0,
        },
        "ordini_attivi": [],
        "ordini_sospesi": [],
    }

    for utente in utenti_subordinati:
        utenti_data[utente.username] = {
            "id": utente.id,
            "username": utente.username,
            "is_current": False,
            "kpi": {
                "attivi": 0,
                "sospesi": 0,
                "ore_lavorazione_attivi": 0.0,
                "ore_lavorazione_sospesi": 0.0,
                "minuti_attrezzaggio_attivi": 0.0,
                "minuti_attrezzaggio_sospesi": 0.0,
                "giorni_impegno_attivi": 0.0,
                "giorni_impegno_attivi_sospesi": 0.0,
            },
            "ordini_attivi": [],
            "ordini_sospesi": [],
        }

    if utenti_data:
        ordini = (
            InputOdp.query.join(InputOdp.runtime_row)
            .filter(
                InputOdpRuntime.Stato_odp.in_(("Attivo", "In Sospeso")),
                InputOdpRuntime.Utente_operazione.in_(list(utenti_data.keys())),
            )
            .all()
        )

        for ordine in ordini:
            runtime = ordine.runtime_row
            if runtime is None:
                continue

            username_operatore = _norm_text(runtime.Utente_operazione)
            if username_operatore not in utenti_data:
                continue

            ore_lavorazione = _order_hours_snapshot_reparto(ordine)
            minuti_attrezzaggio = _safe_float(getattr(ordine, "AttrezzaggioAttivo", ""))

            record = {
                "ordine": f"{_norm_text(ordine.RifRegistraz)}.{_norm_text(ordine.IdRiga)}",
                "descrizione": _norm_text(ordine.DesArt),
                "quantita": _norm_text(ordine.Quantita),
                "risorsa": _first_not_blank(
                    runtime.RisorsaAttiva,
                    InputOdp._active_value_from_phase_list(
                        ordine.CodRisorsaProd,
                        ordine.FaseAttiva,
                    ),
                    default="-",
                ),
                "tempo_lavorazione": round(ore_lavorazione, 2),
                "tempo_attrezzaggio": round(minuti_attrezzaggio, 2),
            }

            stato_runtime = _norm_text(runtime.Stato_odp).lower()

            if stato_runtime == "attivo":
                bucket_key = "ordini_attivi"
                utenti_data[username_operatore]["kpi"]["ore_lavorazione_attivi"] += (
                    ore_lavorazione
                )
                utenti_data[username_operatore]["kpi"][
                    "minuti_attrezzaggio_attivi"
                ] += minuti_attrezzaggio
            else:
                bucket_key = "ordini_sospesi"
                utenti_data[username_operatore]["kpi"]["ore_lavorazione_sospesi"] += (
                    ore_lavorazione
                )
                utenti_data[username_operatore]["kpi"][
                    "minuti_attrezzaggio_sospesi"
                ] += minuti_attrezzaggio

            utenti_data[username_operatore][bucket_key].append(record)

    for payload in utenti_data.values():
        payload["kpi"]["attivi"] = len(payload["ordini_attivi"])
        payload["kpi"]["sospesi"] = len(payload["ordini_sospesi"])
        payload["kpi"]["ore_lavorazione_attivi"] = round(
            payload["kpi"]["ore_lavorazione_attivi"],
            2,
        )
        payload["kpi"]["ore_lavorazione_sospesi"] = round(
            payload["kpi"]["ore_lavorazione_sospesi"],
            2,
        )
        payload["kpi"]["minuti_attrezzaggio_attivi"] = round(
            payload["kpi"]["minuti_attrezzaggio_attivi"],
            2,
        )
        payload["kpi"]["minuti_attrezzaggio_sospesi"] = round(
            payload["kpi"]["minuti_attrezzaggio_sospesi"],
            2,
        )
        ore_attivi_tot = payload["kpi"]["ore_lavorazione_attivi"] + (
            payload["kpi"]["minuti_attrezzaggio_attivi"] / 60.0
        )

        ore_attivi_sospesi_tot = payload["kpi"]["ore_lavorazione_sospesi"] + (
            payload["kpi"]["minuti_attrezzaggio_sospesi"] / 60.0
        )

        payload["kpi"]["giorni_impegno_attivi"] = _hours_to_work_days(ore_attivi_tot)
        payload["kpi"]["giorni_impegno_attivi_sospesi"] = _hours_to_work_days(
            ore_attivi_sospesi_tot
        )

    lista_utenti = sorted(
        utenti_data.values(),
        key=lambda x: (
            0 if x.get("is_current") else 1,
            (x.get("username") or "").lower(),
        ),
    )

    return render_template(
        "dash_reparto.j2",
        utenti_dashboard=lista_utenti,
    )
