from datetime import datetime, date
from zoneinfo import ZoneInfo
import json
import re
import unicodedata
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
    g,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, select, and_, exists, or_
from app_odp.gen_etichette import gen_etichette
from app_odp.models import (
    InputOdp,
    InputOdpRuntime,
    db,
    OdpPriorita,
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
    HomeRepartoConfig,
    HomeVisibilityRule,
    ConfigAuditLog,
    ProductionCapacityCalendar,
)
from app_odp.ordine_ref import format_ordine_ref_display_from_ordine
from app_odp.policy.policy import RbacPolicy
from uuid import uuid4
import win32con
import win32ui
from PIL import Image, ImageOps, ImageWin
from app_odp.operator_session import (
    active_user,
    active_policy,
    active_token,
)
from datetime import timedelta

main_bp = Blueprint("main", __name__)
ROME_TZ = ZoneInfo("Europe/Rome")
MIN_SECONDS_BEFORE_CLOSE_WITHOUT_TIME_PERMISSION = 180


def _format_date_it(day_value: date | None) -> str:
    if not day_value:
        return ""
    return day_value.strftime("%d/%m/%Y")


def _is_business_day(day_value: date) -> bool:
    if day_value.weekday() >= 5:
        return False
    return day_value not in _italian_holidays(day_value.year)


def _home_rows_for_config(
    policy: RbacPolicy,
    config: HomeRepartoConfig,
    *,
    apply_priorita: bool = True,
    sort_priorita: bool = True,
) -> list[InputOdp]:
    odp = list(_query_for_home_config(policy, config).all())

    if apply_priorita:
        odp = _apply_priorita_to_ordini(
            list(odp),
            _current_user_id(),
            sort_result=sort_priorita,
        )

    return odp


def _render_fragments_for_home_config(
    config: HomeRepartoConfig,
    odp: list[InputOdp],
) -> dict:
    renderer_key = _norm_text(getattr(config, "renderer", "")) or "empty"

    if renderer_key == "montaggio":
        method_settings = _home_method_settings_for_user(
            config,
            _current_policy(),
            active_user(),
        )

        return _render_bridge_montaggio(
            odp,
            metodo_path_key=method_settings["path_key"],
            metodo_prefisso=method_settings["prefisso"],
        )

    renderer = RENDERERS.get(renderer_key)
    if renderer is None:
        current_app.logger.warning(
            "Renderer home non valido: tab_code=%s renderer=%s",
            getattr(config, "tab_code", ""),
            renderer_key,
        )
        return {}

    return renderer(odp)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _italian_holidays(year: int) -> set[date]:
    easter = _easter_sunday(year)
    easter_monday = easter + timedelta(days=1)

    return {
        date(year, 1, 1),  # Capodanno
        date(year, 1, 6),  # Epifania
        date(year, 4, 25),  # Liberazione
        date(year, 5, 1),  # Festa del Lavoro
        date(year, 6, 2),  # Festa della Repubblica
        date(year, 8, 15),  # Ferragosto
        date(year, 11, 1),  # Ognissanti
        date(year, 12, 8),  # Immacolata
        date(year, 12, 25),  # Natale
        date(year, 12, 26),  # Santo Stefano
        easter_monday,  # Lunedì dell'Angelo
    }


def _normalize_to_business_day(day_value: date) -> date:
    current = day_value
    while not _is_business_day(current):
        current += timedelta(days=1)
    return current


def _add_business_days(start_day: date, business_days: int) -> date:
    current = _normalize_to_business_day(start_day)

    if business_days <= 0:
        return current

    remaining = int(business_days)

    while remaining > 0:
        current += timedelta(days=1)
        if _is_business_day(current):
            remaining -= 1

    return current


def _calc_supply_date_from_today(lead_time_days) -> date | None:
    try:
        lead_days = int(float(lead_time_days or 0))
    except (TypeError, ValueError):
        return None

    today_rome = _now_rome_dt().date()
    return _add_business_days(today_rome, lead_days)


def _norm_text(value) -> str:
    return str(value or "").strip()


def _current_policy() -> RbacPolicy:
    return active_policy()


def _now_rome_dt() -> datetime:
    return datetime.now(ROME_TZ)


def _current_username(default: str = "utente_sconosciuto") -> str:
    user = active_user()

    return (
        getattr(user, "username", None)
        or getattr(user, "name", None)
        or getattr(user, "email", None)
        or str(getattr(user, "id", default))
    )


def _current_user_id(default: int | None = None):
    user = active_user()
    return getattr(user, "id", default)


def _parse_bool_flag(value) -> bool:
    if isinstance(value, bool):
        return value
    raw = _norm_text(value).lower()
    return raw in {"1", "true", "si", "sì", "yes", "on"}


def _parse_qty_decimal(value) -> Decimal:
    raw = _norm_text(value).replace(",", ".")
    if raw == "":
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation:
        raise ValueError(f"Quantità non valida: {value!r}")


def _snapshot_priorita_in_runtime(
    stato,
    priorita_row: OdpPriorita | None,
    operatore_id: int,
    when_iso: str,
) -> None:
    """
    Salva nel runtime la priorità che l'ordine aveva
    per l'operatore che lo prende in carico.

    Se l'operatore corrente non aveva priorità assegnata,
    lo snapshot viene pulito.
    """
    if stato is None:
        return

    if priorita_row is None:
        stato.PrioritaInCarico = None
        stato.PrioritaOperatoreIdInCarico = None
        stato.PrioritaPresaInCaricoAt = None
        return

    stato.PrioritaInCarico = int(priorita_row.Priorita)
    stato.PrioritaOperatoreIdInCarico = int(operatore_id)
    stato.PrioritaPresaInCaricoAt = when_iso


def _priorita_row_for_operatore_ordine(
    operatore_id: int,
    id_documento: str,
    id_riga: str,
    fase: str,
) -> OdpPriorita | None:
    """
    Recupera la priorità assegnata allo specifico operatore
    per lo specifico ordine/fase.
    """
    key = _make_ordine_fase_key(id_documento, id_riga, fase)

    return (
        OdpPriorita.query.filter_by(
            operatore_id=int(operatore_id),
            IdDocumento=key[0],
            IdRiga=key[1],
            Fase=key[2],
        )
        .order_by(
            OdpPriorita.Priorita.asc(),
            OdpPriorita.Posizione.asc(),
            OdpPriorita.id.asc(),
        )
        .first()
    )


def _cleanup_priorita_operatore(operatore: User) -> None:
    """
    Elimina priorità non più valide:
    - ordine non più visibile all'operatore;
    - ordine non più Pianificata;
    - fase cambiata.
    """
    valid_keys = _priorita_valid_keys_for_operatore(operatore)

    for row in _priorita_rows_for_operatore(operatore.id):
        key = _make_ordine_fase_key(row.IdDocumento, row.IdRiga, row.Fase)
        if key not in valid_keys:
            db.session.delete(row)


def _ordine_fase_key(ordine) -> tuple[str, str, str]:
    return (
        _norm_text(ordine.IdDocumento),
        _norm_text(ordine.IdRiga),
        _norm_text(ordine.FaseAttiva) or "1",
    )


def _priorita_rows_for_operatore(operatore_id: int) -> list[OdpPriorita]:
    return (
        OdpPriorita.query.filter_by(operatore_id=int(operatore_id))
        .order_by(
            OdpPriorita.Priorita.asc(),
            OdpPriorita.Posizione.asc(),
            OdpPriorita.id.asc(),
        )
        .all()
    )


def _priorita_map_for_operatore(
    operatore_id: int,
) -> dict[tuple[str, str, str], OdpPriorita]:
    return {
        _make_ordine_fase_key(row.IdDocumento, row.IdRiga, row.Fase): row
        for row in _priorita_rows_for_operatore(operatore_id)
    }


def _priorita_valid_keys_for_operatore(
    operatore: User,
) -> dict[tuple[str, str, str], InputOdp]:
    return {
        _ordine_fase_key(ordine): ordine
        for ordine in _ordini_pianificata_visibili_per_operatore(operatore)
    }


def _is_ordine_pianificata(ordine) -> bool:
    return _norm_text(getattr(ordine, "StatoOrdine", "")).lower() == "pianificata"


PRIORITA_2_MAX_DEFAULT = 5
PRIORITA_HIDDEN_ROLE_NAMES = {"admin"}


def _priorita_2_max() -> int:
    try:
        return int(current_app.config.get("PRIORITA_2_MAX", PRIORITA_2_MAX_DEFAULT))
    except (TypeError, ValueError):
        return PRIORITA_2_MAX_DEFAULT


def _ordini_pianificata_visibili_per_operatore(operatore: User) -> list[InputOdp]:
    policy_operatore = RbacPolicy(operatore)

    q = _base_odp_query()
    q = policy_operatore.filter_input_odp(q)
    ordini = q.all()

    ordini = policy_operatore.filter_montaggio_macchine_famiglia_rows(ordini)

    return [ordine for ordine in ordini if _is_ordine_pianificata(ordine)]


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


def _ordine_priorita_payload(ordine, priorita_row: OdpPriorita | None = None) -> dict:
    fase = _norm_text(ordine.FaseAttiva) or "1"

    return {
        "key": f"{ordine.IdDocumento}|{ordine.IdRiga}|{fase}",
        "id_documento": _norm_text(ordine.IdDocumento),
        "id_riga": _norm_text(ordine.IdRiga),
        "fase": fase,
        "ordine": _ordine_ref_label(ordine),
        "codice": _norm_text(ordine.CodArt),
        "variante": _norm_text(ordine.VarianteArt),
        "revisione": _norm_text(ordine.IndiceModifica),
        "descrizione": _norm_text(ordine.DesArt),
        "quantita": _norm_text(getattr(ordine, "QtyDaLavorare", ""))
        or _norm_text(ordine.Quantita),
        "risorsa": _norm_text(getattr(ordine, "RisorsaAttiva", "")),
        "lavorazione": _norm_text(getattr(ordine, "LavorazioneAttiva", "")),
        "priorita": priorita_row.Priorita if priorita_row else None,
        "matricola": _norm_text(getattr(ordine, "CodMatricola", "")),
        "posizione": priorita_row.Posizione if priorita_row else None,
    }


def _compact_priorita_operatore(operatore_id: int) -> None:
    """
    Ricompatta la coda:
    - 1 solo ordine in priorità 1;
    - massimo N ordini in priorità 2;
    - resto in priorità 3.
    """
    rows = _priorita_rows_for_operatore(operatore_id)
    max_p2 = _priorita_2_max()

    for index, row in enumerate(rows):
        if index == 0:
            row.Priorita = 1
            row.Posizione = 1
        elif index <= max_p2:
            row.Priorita = 2
            row.Posizione = index
        else:
            row.Priorita = 3
            row.Posizione = index - max_p2

        row.updated_at = _priority_now_iso()


def _consume_priorita_ordine(id_documento: str, id_riga: str, fase: str) -> None:
    """
    Da chiamare quando un ordine viene preso in carico.
    Rimuove quell'ordine da tutte le code operatore in cui compare,
    poi ricompatta le code coinvolte.
    """
    key = _make_ordine_fase_key(id_documento, id_riga, fase)

    rows = OdpPriorita.query.filter_by(
        IdDocumento=key[0],
        IdRiga=key[1],
        Fase=key[2],
    ).all()

    operatori_coinvolti = {row.operatore_id for row in rows}

    for row in rows:
        db.session.delete(row)

    db.session.flush()

    for operatore_id in operatori_coinvolti:
        _compact_priorita_operatore(operatore_id)


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


def _json_safe(value):
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, set):
        return sorted(_json_safe(v) for v in value)
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


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


def _first_code_from_cell(value) -> str:
    for code in _extract_codes_from_cell(value):
        code = _norm_text(code)
        if code:
            return code
    return ""


def _first_not_blank(*values, default=""):
    for value in values:
        text = _norm_text(value)
        if text:
            return text
    return default


def _bool_text(value: bool) -> str:
    return "true" if bool(value) else "false"


def _row_key(id_documento: str, id_riga: str) -> str:
    return f"{id_documento}|{id_riga}"


def _build_rif_ordine_princ(id_documento: str, id_riga: str) -> str:
    return json.dumps(
        [_norm_text(id_documento), _norm_text(id_riga)],
        ensure_ascii=False,
    )


def _ordine_ref_label(ordine) -> str:
    ref = format_ordine_ref_display_from_ordine(ordine)

    if ref:
        return ref

    return f"{_norm_text(ordine.IdDocumento)} {_norm_text(ordine.IdRiga)}".strip()


def _base_odp_query():
    return InputOdp.query.options(
        selectinload(InputOdp.runtime_row),
    )


def filter_input_odp_for_home_config(
    query,
    config: HomeRepartoConfig,
    policy: RbacPolicy,
    user=None,
):
    return policy.filter_input_odp_for_home_config(
        query,
        config,
        user=user,
    )


def _last_log_token() -> str:
    runtime_max = db.session.query(func.max(OdpRuntimeLog.log_id)).scalar() or 0
    input_max = db.session.query(func.max(InputOdpLog.log_id)).scalar() or 0
    return f"{input_max}:{runtime_max}"


@main_bp.context_processor
def inject_policy_and_nav():
    user = active_user()

    if not getattr(user, "is_authenticated", False):
        return {}

    policy = active_policy()

    operator_token = _norm_text(request.args.get("tab_session")) or active_token()

    items = []

    for cfg in _allowed_home_reparto_configs(policy):
        url_kwargs = {"tab": cfg.tab_code}
        if operator_token:
            url_kwargs["tab_session"] = operator_token

        items.append(
            {
                "label": _home_reparto_label(cfg),
                "url": url_for(".home", **url_kwargs),
                "tab": cfg.tab_code,
                "reparto": _home_reparto_code(cfg),
            }
        )

    area_switch_items = []

    if policy.can("home_acquisti"):
        acq_kwargs = {}
        if operator_token:
            acq_kwargs["tab_session"] = operator_token

        area_switch_items.append(
            {
                "label": "Acquisti",
                "url": url_for(".home_acquisti", **acq_kwargs),
                "area": "acquisti",
            }
        )

    if policy.can("home"):
        first_production_tab = None

        for it in items:
            first_production_tab = it["tab"]
            break

        if first_production_tab:
            prod_kwargs = {"tab": first_production_tab}
            if operator_token:
                prod_kwargs["tab_session"] = operator_token

            area_switch_items.append(
                {
                    "label": "Produzione",
                    "url": url_for(".home", **prod_kwargs),
                    "area": "produzione",
                }
            )

    return {
        "policy": policy,
        "operator_user": getattr(g, "operator_user", None),
        "operator_policy": getattr(g, "operator_policy", None),
        "tab_session": operator_token,
        "home_switch_items": items,
        "area_switch_items": area_switch_items,
    }


@main_bp.context_processor
def inject_order_ref_formatters():
    return {
        "ordine_ref_display": format_ordine_ref_display_from_ordine,
    }


def _normalize_home_tab_code(value) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    raw = unicodedata.normalize("NFKD", raw)
    raw = raw.encode("ascii", "ignore").decode("ascii")
    raw = raw.strip().lower()
    raw = re.sub(r"[\s\-]+", "_", raw)
    raw = re.sub(r"[^a-z0-9_]+", "", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")

    return raw


def _home_reparto_configs_query():
    return (
        HomeRepartoConfig.query.options(
            selectinload(HomeRepartoConfig.reparto),
        )
        .filter(HomeRepartoConfig.attivo.is_(True))
        .order_by(
            HomeRepartoConfig.ordine_menu.asc(),
            func.lower(HomeRepartoConfig.label).asc(),
            HomeRepartoConfig.id.asc(),
        )
    )


def _home_reparto_config_by_tab(tab_code: str):
    tab_norm = _normalize_home_tab_code(tab_code)
    if not tab_norm:
        return None

    return (
        HomeRepartoConfig.query.options(
            selectinload(HomeRepartoConfig.reparto),
        )
        .filter(HomeRepartoConfig.attivo.is_(True))
        .filter(func.lower(HomeRepartoConfig.tab_code) == tab_norm)
        .first()
    )


def _home_reparto_code(config: HomeRepartoConfig) -> str:
    if config is None or config.reparto is None:
        return ""

    return _norm_text(config.reparto.Codice)


def _home_reparto_label(config: HomeRepartoConfig) -> str:
    if config is None:
        return ""

    return (
        _norm_text(config.label)
        or _norm_text(getattr(config.reparto, "Descrizione", ""))
        or _norm_text(getattr(config.reparto, "Codice", ""))
        or _norm_text(config.tab_code)
    )


def _policy_can_access_home_config(
    policy: RbacPolicy, config: HomeRepartoConfig
) -> bool:
    if policy is None or config is None:
        return False

    reparto_code = _home_reparto_code(config)
    permesso = _norm_text(config.permesso) or "home"

    if not reparto_code:
        return False

    if reparto_code not in policy.allowed_reparti:
        return False

    if not policy.can(permesso):
        return False

    return True


def _allowed_home_reparto_configs(policy: RbacPolicy) -> list[HomeRepartoConfig]:
    configs = _home_reparto_configs_query().all()

    return [cfg for cfg in configs if _policy_can_access_home_config(policy, cfg)]


def _first_allowed_home_reparto_config(policy: RbacPolicy):
    rows = _allowed_home_reparto_configs(policy)
    return rows[0] if rows else None


def _home_reparto_config_for_ordine(ordine: InputOdp):
    reparto_codes = set(_extract_codes_from_cell(getattr(ordine, "CodReparto", "")))
    if not reparto_codes:
        return None

    configs = _home_reparto_configs_query().all()

    for cfg in configs:
        if _home_reparto_code(cfg) in reparto_codes:
            return cfg

    return None


def _query_for_home_config(policy: RbacPolicy, config: HomeRepartoConfig):
    return filter_input_odp_for_home_config(
        _base_odp_query(),
        config,
        policy,
        active_user(),
    )


def _home_rows_for_config(
    policy: RbacPolicy,
    config: HomeRepartoConfig,
    *,
    apply_priorita: bool = True,
    sort_priorita: bool = True,
) -> list[InputOdp]:
    odp = list(_query_for_home_config(policy, config).all())

    if apply_priorita:
        odp = _apply_priorita_to_ordini(
            list(odp),
            _current_user_id(),
            sort_result=sort_priorita,
        )

    return odp


def _query_for_tab(policy, reparto_code):
    q = _base_odp_query()
    q = policy.filter_input_odp_for_reparto(q, reparto_code)
    return q


def _home_ui_texts_for_user(
    config: HomeRepartoConfig,
    policy: RbacPolicy,
    user=None,
) -> dict:
    user = user or active_user()

    texts = {
        "titolo_macchine_da_eseguire": "Ordini macchina da eseguire",
        "titolo_macchine_attive": "Ordini macchina attivi",
        "titolo_semilavorati_da_eseguire": "Ordini semilavorati da eseguire",
        "titolo_semilavorati_attivi": "Ordini semilavorati attivi",
        "testo_presa_macchina": "Attiva ordine macchina",
        "testo_sospendi_macchina": "Sospendi ordine",
        "testo_riattiva_macchina": "Riattiva ordine",
        "testo_chiudi_macchina": "Chiudi ordine",
        "toast_presa_macchina": "Presa in carico ordine macchina",
        "toast_sospendi_macchina": "Sospensione ordine macchina",
        "toast_riattiva_macchina": "Riattivazione ordine macchina",
        "toast_chiudi_macchina": "Chiusura ordine macchina",
    }

    config_map = {
        "titolo_macchine_da_eseguire": getattr(
            config, "titolo_macchine_da_eseguire", None
        ),
        "titolo_macchine_attive": getattr(config, "titolo_macchine_attive", None),
        "titolo_semilavorati_da_eseguire": getattr(
            config, "titolo_semilavorati_da_eseguire", None
        ),
        "titolo_semilavorati_attivi": getattr(
            config, "titolo_semilavorati_attivi", None
        ),
        "testo_presa_macchina": getattr(config, "testo_presa_macchina", None),
        "testo_sospendi_macchina": getattr(config, "testo_sospendi_macchina", None),
        "testo_riattiva_macchina": getattr(config, "testo_riattiva_macchina", None),
        "testo_chiudi_macchina": getattr(config, "testo_chiudi_macchina", None),
    }

    for key, value in config_map.items():
        value = _norm_text(value)
        if value:
            texts[key] = value

    rules = (
        HomeVisibilityRule.query.filter(
            HomeVisibilityRule.attivo.is_(True),
            HomeVisibilityRule.reparto_id == config.reparto_id,
        )
        .filter(
            or_(
                HomeVisibilityRule.role_id.in_(policy.role_ids),
                HomeVisibilityRule.user_id == getattr(user, "id", None),
            )
        )
        .order_by(
            HomeVisibilityRule.user_id.isnot(None).asc(),
            HomeVisibilityRule.id.asc(),
        )
        .all()
    )

    for rule in rules:
        rule_map = {
            "titolo_macchine_da_eseguire": rule.titolo_macchine_da_eseguire,
            "titolo_macchine_attive": rule.titolo_macchine_attive,
            "testo_presa_macchina": rule.testo_presa_macchina,
            "testo_sospendi_macchina": rule.testo_sospendi_macchina,
            "testo_riattiva_macchina": rule.testo_riattiva_macchina,
            "testo_chiudi_macchina": rule.testo_chiudi_macchina,
        }

        for key, value in rule_map.items():
            value = _norm_text(value)
            if value:
                texts[key] = value

    # Toast: se non hai un campo dedicato, riuso gli stessi testi azione.
    texts["toast_presa_macchina"] = (
        texts.get("testo_presa_macchina") or texts["toast_presa_macchina"]
    )
    texts["toast_sospendi_macchina"] = (
        texts.get("testo_sospendi_macchina") or texts["toast_sospendi_macchina"]
    )
    texts["toast_riattiva_macchina"] = (
        texts.get("testo_riattiva_macchina") or texts["toast_riattiva_macchina"]
    )
    texts["toast_chiudi_macchina"] = (
        texts.get("testo_chiudi_macchina") or texts["toast_chiudi_macchina"]
    )

    return texts


def _home_method_settings_for_user(
    config: HomeRepartoConfig,
    policy: RbacPolicy,
    user=None,
) -> dict:
    user = user or active_user()

    settings = {
        "tipo": _norm_text(getattr(config, "metodo_documentale_tipo", ""))
        or "montaggio",
        "prefisso": _norm_text(getattr(config, "metodo_documentale_prefisso", "")),
        "path_key": _norm_text(getattr(config, "metodo_documentale_path_key", ""))
        or "MONTAGGIO_PDF_DIR",
    }

    rules = (
        HomeVisibilityRule.query.filter(
            HomeVisibilityRule.attivo.is_(True),
            HomeVisibilityRule.reparto_id == config.reparto_id,
        )
        .filter(
            or_(
                HomeVisibilityRule.role_id.in_(policy.role_ids),
                HomeVisibilityRule.user_id == getattr(user, "id", None),
            )
        )
        .order_by(
            HomeVisibilityRule.user_id.isnot(None).asc(),
            HomeVisibilityRule.id.asc(),
        )
        .all()
    )

    for rule in rules:
        if _norm_text(rule.metodo_documentale_tipo):
            settings["tipo"] = _norm_text(rule.metodo_documentale_tipo)
        if _norm_text(rule.metodo_documentale_prefisso):
            settings["prefisso"] = _norm_text(rule.metodo_documentale_prefisso)
        if _norm_text(rule.metodo_documentale_path_key):
            settings["path_key"] = _norm_text(rule.metodo_documentale_path_key)

    return settings


def _render_bridge_standard(odp):
    from app_odp.services.documenti_service import _build_metodo_montaggio_lookup

    metodo_montaggio_lookup = _build_metodo_montaggio_lookup(odp)

    return {
        "tbody_ordini_da_eseguire": render_template(
            "partials/_home_standard_rows_da_eseguire.j2",
            odp=odp,
        ),
        "tbody_ordini_in_corso": render_template(
            "partials/_home_standard_rows_in_corso.j2",
            odp=odp,
            metodo_montaggio_lookup=metodo_montaggio_lookup,
        ),
    }


def _render_bridge_montaggio(
    odp,
    *,
    metodo_path_key: str = "MONTAGGIO_PDF_DIR",
    metodo_prefisso: str = "",
):
    from app_odp.services.documenti_service import _build_metodo_lookup

    metodo_lookup = _build_metodo_lookup(
        odp,
        path_key=metodo_path_key,
        prefisso=metodo_prefisso,
    )

    ctx = {
        "odp": odp,
        "metodo_lookup": metodo_lookup,
        "metodo_documentale_prefisso": metodo_prefisso,
    }

    return {
        "tbody_ordini_da_eseguire_sl": render_template(
            "partials/_home_montaggio_sl_rows_da_eseguire.j2",
            odp=odp,
        ),
        "tbody_ordini_in_corso_sl": render_template(
            "partials/_home_montaggio_sl_rows_in_corso.j2",
            **ctx,
        ),
        "tbody_ordini_da_eseguire_m": render_template(
            "partials/_home_montaggio_m_rows_da_eseguire.j2",
            odp=odp,
        ),
        "tbody_ordini_in_corso_m": render_template(
            "partials/_home_montaggio_m_rows_in_corso.j2",
            **ctx,
        ),
    }


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


def _render_bridge_empty(odp):
    return {}


RENDERERS = {
    "standard": _render_bridge_standard,
    "montaggio": _render_bridge_montaggio,
    "empty": _render_bridge_empty,
}


def _render_fragments_for_home_config(
    config: HomeRepartoConfig,
    odp: list[InputOdp],
) -> dict:
    renderer_key = _norm_text(getattr(config, "renderer", "")) or "empty"

    if renderer_key == "montaggio":
        method_settings = _home_method_settings_for_user(
            config,
            _current_policy(),
            active_user(),
        )

        return _render_bridge_montaggio(
            odp,
            metodo_path_key=method_settings["path_key"],
            metodo_prefisso=method_settings["prefisso"],
        )

    renderer = RENDERERS.get(renderer_key)
    if renderer is None:
        current_app.logger.warning(
            "Renderer home non valido: tab_code=%s renderer=%s",
            getattr(config, "tab_code", ""),
            renderer_key,
        )
        return {}

    return renderer(odp)


def _tab_from_ordine(ordine: InputOdp) -> str | None:
    config = _home_reparto_config_for_ordine(ordine)
    return config.tab_code if config else None


def _get_visible_odp_by_key(
    policy: RbacPolicy,
    id_documento: str,
    id_riga: str,
) -> InputOdp:
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

    config = _home_reparto_config_for_ordine(exists_anyway)

    if config is None:
        abort(403)

    if not _policy_can_access_home_config(policy, config):
        abort(403)

    ordine = (
        filter_input_odp_for_home_config(
            _base_odp_query(),
            config,
            policy,
            active_user(),
        )
        .filter(
            InputOdp.IdDocumento == id_documento,
            InputOdp.IdRiga == id_riga,
        )
        .first()
    )

    if ordine is None:
        abort(403)

    return ordine


def _fragments_for_ordine_tab(
    policy: RbacPolicy,
    ordine: InputOdp,
) -> tuple[str | None, dict]:
    config = _home_reparto_config_for_ordine(ordine)
    if config is None:
        return None, {}

    if not _policy_can_access_home_config(policy, config):
        return None, {}

    odp = _home_rows_for_config(policy, config, apply_priorita=True, sort_priorita=True)
    fragments = _render_fragments_for_home_config(config, odp)

    return config.tab_code, fragments


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


def _ordine_has_distinta_materiale(ordine) -> bool:
    distinta = _parse_distinta_materiale(ordine)
    return any(isinstance(comp, dict) for comp in distinta)


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
        variante_art = _norm_text(comp.get("VarianteArt", ""))

        chiave_componente = (cod_art, variante_art)

        if not cod_art or chiave_componente in codici_visti:
            continue

        codici_visti.add(chiave_componente)

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
                    "VarianteArt": _norm_text(comp.get("VarianteArt", "")),
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
                "VarianteArt": _norm_text(row.get("VarianteArt")),
                "RifLottoAlfa": _norm_text(row.get("RifLottoAlfa")),
                "CodMag": _norm_text(row.get("CodMag")),
                "Quantita": str(row.get("Quantita", 0)),
                "Esito": _norm_text(row.get("Esito", "ok")),
            }
        )
    return rows


def _normalize_lotto_prodotto_for_payload(lotto: dict | None) -> dict | None:
    if not lotto:
        return None

    return _norm_text(lotto.get("RifLottoAlfa"))


def _reset_runtime_for_next_phase(
    stato,
    ordine,
    username: str,
    next_phase: str,
):
    """
    Prepara il runtime per la fase successiva.

    Il tempo deve ripartire da zero, perché Tempo_funzionamento
    deve rappresentare il tempo della fase corrente, non il cumulato ordine.
    """
    if stato is None:
        return

    next_phase = _norm_text(next_phase)

    stato.Stato_odp = "Pianificata"
    stato.Utente_operazione = username
    stato.FaseAttiva = next_phase

    # Punto centrale della modifica:
    # azzera il tempo al cambio fase.
    stato.Tempo_funzionamento = "0"

    # La fase successiva non è ancora presa in carico.
    stato.data_ultima_attivazione = None
    stato.Data_in_carico = None

    stato.QtyDaLavorare = _norm_text(getattr(ordine, "QtyDaLavorare", ""))

    stato.RisorsaAttiva = _norm_text(getattr(ordine, "RisorsaAttiva", ""))
    stato.LavorazioneAttiva = _norm_text(getattr(ordine, "LavorazioneAttiva", ""))
    stato.AttrezzaggioAttivo = _norm_text(getattr(ordine, "AttrezzaggioAttivo", ""))

    stato.VarianteArt = _norm_text(getattr(ordine, "VarianteArt", ""))


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
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
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
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
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
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
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
    label_filename: str = "",
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
            LabelFilename=_norm_text(label_filename),
        ),
    )


def _phase_export_flags(
    ordine,
    fase_corrente: str,
    *,
    chiusura_parziale: bool = False,
) -> dict:
    """
    Determina se questa fase deve generare product_line nel TXT ERP.

    Regola:
    - chiusura parziale: mai product_line
    - monofase: product_line
    - multifase: product_line solo sull'ultima fase
    """
    is_last_phase, next_phase = _get_phase_transition(ordine, fase_corrente)
    phase_sequence = _phase_sequence_for_ordine(ordine)

    return {
        "is_last_phase": bool(is_last_phase),
        "fase_successiva": next_phase or "",
        "phase_sequence": phase_sequence,
        "emit_product_line": bool(is_last_phase and not chiusura_parziale),
    }


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
    registrazione_data: str = "",
    chiusura_parziale: bool = False,
    tipo_documento: str = "",
    risorsa: str = "",
    magazzino: str = "",
    variante: str = "",
    include_time_line: bool = True,
    emit_product_line: bool | None = None,
    is_last_phase: bool | None = None,
    fase_successiva: str | None = None,
    phase_sequence: list[str] | None = None,
) -> dict:
    salda_riga = 0 if chiusura_parziale is True else 1
    if is_last_phase is None or fase_successiva is None:
        calc_is_last_phase, calc_next_phase = _get_phase_transition(
            ordine,
            fase_corrente,
        )

        if is_last_phase is None:
            is_last_phase = calc_is_last_phase

        if fase_successiva is None:
            fase_successiva = calc_next_phase or ""

    if phase_sequence is None:
        phase_sequence = _phase_sequence_for_ordine(ordine)

    if emit_product_line is None:
        emit_product_line = bool(is_last_phase and not chiusura_parziale)
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
        "registrazione_data": registrazione_data,
        "salda_riga": salda_riga,
        "tipo_documento": tipo_documento,
        "risorsa": risorsa,
        "magazzino": magazzino,
        "distinta_base": distinta_base,
        "variante": variante,
        "num_progr_riga": ordine.NumProgrRiga,
        "include_time_line": bool(include_time_line),
        "emit_product_line": bool(emit_product_line),
        "is_last_phase": bool(is_last_phase),
        "fase_successiva": _norm_text(fase_successiva),
        "phase_sequence": phase_sequence,
        "num_fase": ordine.NumFase,
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


def _build_export_distinta_base(
    ordine,
    fase_corrente: str,
    q_lavorata: Decimal,
    q_tot: Decimal,
) -> str:
    distinta = _parse_distinta_materiale(ordine)
    fase_corrente_int = _fase_to_int(fase_corrente)

    out = []

    for comp in distinta:
        if not isinstance(comp, dict):
            continue

        comp_fase = _fase_to_int(comp.get("NumFase"))
        if fase_corrente_int is not None and comp_fase != fase_corrente_int:
            continue

        qty_scalata = _scaled_component_qty(
            comp.get("Quantita"),
            q_lavorata=q_lavorata,
            q_tot=q_tot,
        )

        out.append(
            {
                **comp,
                "Quantita": _decimal_to_text(qty_scalata),
                "VarianteArt": _norm_text(comp.get("VarianteArt", "")),
            }
        )

    return json.dumps(out, ensure_ascii=False)


def _restore_priorita_for_next_phase_from_runtime(
    stato,
    ordine,
    next_phase: str | None,
) -> None:
    """
    Se un ordine prioritizzato avanza alla fase successiva,
    ricrea la priorità sulla nuova fase per lo stesso operatore.

    Non compatta la coda: mantiene il numero priorità originale.
    """
    if stato is None or not next_phase:
        return

    priorita = getattr(stato, "PrioritaInCarico", None)
    operatore_id = getattr(stato, "PrioritaOperatoreIdInCarico", None)

    if priorita not in (1, 2, 3) or not operatore_id:
        return

    key = _make_ordine_fase_key(
        ordine.IdDocumento,
        ordine.IdRiga,
        next_phase,
    )

    existing = OdpPriorita.query.filter_by(
        operatore_id=int(operatore_id),
        IdDocumento=key[0],
        IdRiga=key[1],
        Fase=key[2],
    ).first()

    now_iso = _priority_now_iso()

    max_posizione = (
        db.session.query(func.max(OdpPriorita.Posizione))
        .filter_by(
            operatore_id=int(operatore_id),
            Priorita=int(priorita),
        )
        .scalar()
        or 0
    )

    if existing is not None:
        existing.Priorita = int(priorita)
        existing.Posizione = int(max_posizione) + 1
        existing.updated_at = now_iso
        existing.updated_by = _current_username("priorita_fase_successiva")
        return

    db.session.add(
        OdpPriorita(
            operatore_id=int(operatore_id),
            IdDocumento=key[0],
            IdRiga=key[1],
            Fase=key[2],
            Priorita=int(priorita),
            Posizione=int(max_posizione) + 1,
            created_at=now_iso,
            updated_at=now_iso,
            updated_by=_current_username("priorita_fase_successiva"),
        )
    )


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
        VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
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
    Cancella InputOdpRuntime solo se la riga esiste ancora, poi cancella InputOdp.
    """
    id_documento = _norm_text(getattr(ordine, "IdDocumento", ""))
    id_riga = _norm_text(getattr(ordine, "IdRiga", ""))

    if id_documento and id_riga:
        (
            db.session.query(InputOdpRuntime)
            .filter(
                InputOdpRuntime.IdDocumento == id_documento,
                InputOdpRuntime.IdRiga == id_riga,
            )
            .delete(synchronize_session=False)
        )

    if ordine is not None:
        db.session.delete(ordine)

    db.session.flush()


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
            VarianteArt=_norm_text(getattr(ordine, "VarianteArt", "")),
        )
        db.session.add(stato)
        return stato

    stato.Stato_odp = "Attivo"
    stato.Utente_operazione = username

    fase_precedente = _norm_text(getattr(stato, "FaseAttiva", ""))

    if fase_corrente:
        if fase_precedente and fase_precedente != _norm_text(fase_corrente):
            stato.Tempo_funzionamento = "0"
            stato.Data_in_carico = None
            stato.data_ultima_attivazione = None

        stato.FaseAttiva = fase_corrente
    if not _norm_text(stato.Data_in_carico):
        stato.Data_in_carico = now_iso
    if not _norm_text(stato.Tempo_funzionamento):
        stato.Tempo_funzionamento = "0"
    if rif_ordine_princ is not None:
        stato.RifOrdinePrinc = rif_ordine_princ
    stato.VarianteArt = _norm_text(getattr(ordine, "VarianteArt", ""))
    stato.data_ultima_attivazione = now_iso
    return stato


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


def _ensure_min_active_time_before_chiusura(
    stato,
    now_dt: datetime,
    *,
    can_bypass: bool,
    min_seconds: int = MIN_SECONDS_BEFORE_CLOSE_WITHOUT_TIME_PERMISSION,
):
    """
    Impedisce la chiusura troppo rapida agli operatori senza permission
    export_avp_senza_riga_tempo.

    Usa data_ultima_attivazione come riferimento principale.
    Se manca, usa Data_in_carico come fallback.
    """
    if can_bypass:
        return None

    if stato is None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Ordine non chiudibile: runtime ordine non trovato. "
                        "Riattivare l'ordine prima della chiusura."
                    ),
                }
            ),
            409,
        )

    start_dt = _parse_iso_dt(
        getattr(stato, "data_ultima_attivazione", "")
    ) or _parse_iso_dt(getattr(stato, "Data_in_carico", ""))

    if start_dt is None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Ordine non chiudibile: data di attivazione non disponibile. "
                        "Riattivare l'ordine e attendere almeno 3 minuti prima della chiusura."
                    ),
                }
            ),
            409,
        )

    elapsed_seconds = max(0, int((now_dt - start_dt).total_seconds()))

    if elapsed_seconds >= min_seconds:
        return None

    remaining_seconds = min_seconds - elapsed_seconds
    remaining_minutes = (remaining_seconds + 59) // 60

    return (
        jsonify(
            {
                "ok": False,
                "error": (
                    "Ordine non chiudibile: attendere almeno 3 minuti "
                    "dalla presa in carico o dall'ultima riattivazione. "
                    f"Tempo residuo circa {remaining_minutes} min."
                ),
            }
        ),
        409,
    )


def _ensure_ordine_attivo_per_chiusura(ordine, stato=None):
    stato_attuale = _stato_operativo_chiusura(ordine, stato=stato)
    stato_norm = stato_attuale.lower()

    if stato_norm == "attivo":
        return None

    if stato_norm == "in sospeso":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": (
                        "Ordine non chiudibile: è in sospeso. "
                        "Riattiva l'ordine prima della chiusura."
                    ),
                }
            ),
            409,
        )

    if stato_norm == "pianificata":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Ordine non chiudibile: è ancora Pianificata.",
                }
            ),
            409,
        )

    return (
        jsonify(
            {
                "ok": False,
                "error": f"Ordine non chiudibile: stato attuale '{stato_attuale or '-'}'.",
            }
        ),
        409,
    )


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


def generazione_lotti(dt=None) -> str:
    dt = dt or _now_rome_dt()
    return dt.strftime("%Y%m%d")


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
    encoding: str = "utf-8",
) -> Path:
    path_txt = _build_export_txt_path(prefix=prefix, suffix=suffix)
    content = "\n".join(lines) + "\n"
    path_txt.write_text(content, encoding=encoding, newline="\r\n")
    return path_txt


def _json_loads_safe(raw, default):
    try:
        return json.loads(raw)
    except Exception:
        return default


def _get_pending_avp_outbox(outbox_id: int | None = None) -> list[ErpOutbox]:
    q = ErpOutbox.query.filter(
        ErpOutbox.kind == "consuntivo_fase",
        ErpOutbox.status == "pending",
    )

    if outbox_id is not None:
        q = q.filter(ErpOutbox.outbox_id == outbox_id)

    return q.order_by(ErpOutbox.outbox_id.asc()).all()


def _get_outbox_payload(outbox: ErpOutbox) -> dict:
    payload = _json_loads_safe(outbox.payload_json or "{}", {})
    return payload if isinstance(payload, dict) else {}


def _get_pending_avp_export_rows(outbox_id: int | None = None) -> list[dict]:
    rows = []
    for outbox in _get_pending_avp_outbox(outbox_id=outbox_id):
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


def _role_config_items_for_creation(policy: RbacPolicy, cfg: dict) -> list[dict]:
    model = cfg["model"]
    code_attr = cfg["code_attr"]
    desc_attr = cfg["desc_attr"]

    if model is Roles:
        rows = policy.role_creation_manageable_roles()
    else:
        rows = model.query.order_by(
            func.lower(
                func.coalesce(
                    getattr(model, desc_attr),
                    getattr(model, code_attr),
                )
            ),
            func.lower(getattr(model, code_attr)),
        ).all()

    return [
        {
            "id": getattr(row, cfg["model_id"]),
            "codice": getattr(row, code_attr, "") or "",
            "descrizione": getattr(row, desc_attr, "") or "",
        }
        for row in rows
    ]


def _valid_role_creation_ids(policy: RbacPolicy, cfg: dict) -> set[int]:
    model = cfg["model"]

    if model is Roles:
        return {int(role.id) for role in policy.role_creation_manageable_roles()}

    model_id_attr = getattr(model, cfg["model_id"])
    stmt = select(model_id_attr)
    return {int(x) for x in db.session.execute(stmt).scalars().all()}


def _normalize_role_creation_links(raw_links) -> dict[str, set[int]]:
    if raw_links in (None, ""):
        return {}

    if not isinstance(raw_links, dict):
        raise ValueError("Il payload 'links' deve essere un oggetto JSON.")

    normalized = {}

    for key, raw_values in raw_links.items():
        if key not in ROLE_LINK_CONFIG:
            raise ValueError(f"Tabella non valida nel payload: {key}")

        if raw_values in (None, ""):
            normalized[key] = set()
            continue

        if not isinstance(raw_values, (list, tuple, set)):
            raise ValueError(f"I valori per '{key}' devono essere una lista di id.")

        try:
            normalized[key] = {int(x) for x in raw_values}
        except (TypeError, ValueError):
            raise ValueError(f"Gli id per '{key}' non sono validi.")

    return normalized


def _normalize_id_list(raw_values) -> list[int]:
    if raw_values in (None, ""):
        return []

    if not isinstance(raw_values, (list, tuple, set)):
        raise ValueError("I valori devono essere una lista di id.")

    out = []
    for value in raw_values:
        try:
            item_id = int(value)
        except (TypeError, ValueError):
            raise ValueError("Id non valido.")

        if item_id > 0 and item_id not in out:
            out.append(item_id)

    return out


HOME_CONFIG_TEMPLATE_OPTIONS = {
    "partials/_home_montaggio.j2": "Layout montaggio/macchine",
    "partials/_home_standard.j2": "Layout standard",
    "partials/page_vuota.html": "Pagina vuota",
}

HOME_CONFIG_RENDERER_OPTIONS = {
    "montaggio": "Montaggio/macchine",
    "standard": "Standard",
    "empty": "Vuoto",
}

HOME_CONFIG_METODO_OPTIONS = {
    "montaggio": "Metodo montaggio",
    "collaudo": "Metodo collaudo",
    "nessuno": "Nessuno",
}

HOME_RULE_APPLY_TO_OPTIONS = {
    "macchine": "Macchine",
    "semilavorati": "Semilavorati",
    "all": "Tutto",
}

HOME_RULE_PHASE_MODE_OPTIONS = {
    "all": "Tutte",
    "exact": "Esatta",
    "last": "Ultima",
    "not_first": "Dopo la prima",
    "list": "Lista",
}


def _build_public_id_from_full_name(value: str) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    normalized = unicodedata.normalize("NFKD", raw)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower().strip()

    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_]", "", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")

    return normalized


def _login_code_error_response(message: str, status_code: int = 400):
    return jsonify(
        {
            "ok": False,
            "error": message,
        }
    ), status_code


def _is_login_code_integrity_error(exc: IntegrityError) -> bool:
    msg = str(getattr(exc, "orig", exc)).lower()
    return "unique constraint failed" in msg and "login_code_lookup" in msg


LOGIN_CODE_DUPLICATO_MSG = "Il codice di login è già utilizzato"


def _prepare_login_code_or_response(
    raw_code,
    exclude_user_id: int | None = None,
):
    """
    Valida il login code e verifica che non sia già assegnato.

    exclude_user_id serve nel reset/modifica:
    - permette allo stesso utente di mantenere lo stesso codice;
    - blocca il codice se appartiene a un altro utente.
    """
    try:
        code = User.validate_login_code(raw_code)
    except ValueError as exc:
        return None, _login_code_error_response(str(exc), 400)

    lookup = User.login_code_lookup_for(code)

    query = User.query.filter(User.login_code_lookup == lookup)

    if exclude_user_id is not None:
        query = query.filter(User.id != int(exclude_user_id))

    if query.first() is not None:
        return None, _login_code_error_response(LOGIN_CODE_DUPLICATO_MSG, 409)

    return code, None


def _home_config_bool(value) -> bool:
    return _parse_bool_flag(value)


def _home_config_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _home_config_text(value) -> str:
    return _norm_text(value)


def _home_config_json_payload(obj) -> str:
    return json.dumps(_json_safe(obj), ensure_ascii=False, sort_keys=True)


def _home_config_audit(
    *,
    entity_type: str,
    entity_id: str,
    action: str,
    old_payload=None,
    new_payload=None,
    note: str = "",
):
    user = active_user()

    db.session.add(
        ConfigAuditLog(
            entity_type=entity_type,
            entity_id=str(entity_id),
            action=action,
            old_payload=_home_config_json_payload(old_payload)
            if old_payload is not None
            else None,
            new_payload=_home_config_json_payload(new_payload)
            if new_payload is not None
            else None,
            changed_by_user_id=getattr(user, "id", None),
            changed_by_username=getattr(user, "username", None) or "",
            changed_at=_now_rome_dt().isoformat(timespec="seconds"),
            note=note,
        )
    )


def _home_reparto_config_to_dict(row: HomeRepartoConfig) -> dict:
    return {
        "id": row.id,
        "reparto_id": row.reparto_id,
        "reparto_codice": row.reparto.Codice if row.reparto else "",
        "reparto_descrizione": row.reparto.Descrizione if row.reparto else "",
        "tab_code": row.tab_code or "",
        "label": row.label or "",
        "template": row.template or "",
        "renderer": row.renderer or "",
        "permesso": row.permesso or "home",
        "ordine_menu": int(row.ordine_menu or 0),
        "attivo": bool(row.attivo),
        "titolo_macchine_da_eseguire": row.titolo_macchine_da_eseguire or "",
        "titolo_macchine_attive": row.titolo_macchine_attive or "",
        "titolo_semilavorati_da_eseguire": row.titolo_semilavorati_da_eseguire or "",
        "titolo_semilavorati_attivi": row.titolo_semilavorati_attivi or "",
        "testo_presa_macchina": row.testo_presa_macchina or "",
        "testo_sospendi_macchina": row.testo_sospendi_macchina or "",
        "testo_riattiva_macchina": row.testo_riattiva_macchina or "",
        "testo_chiudi_macchina": row.testo_chiudi_macchina or "",
        "metodo_documentale_tipo": row.metodo_documentale_tipo or "nessuno",
        "metodo_documentale_prefisso": row.metodo_documentale_prefisso or "",
        "metodo_documentale_path_key": row.metodo_documentale_path_key or "",
    }


def _home_visibility_rule_to_dict(row: HomeVisibilityRule) -> dict:
    return {
        "id": row.id,
        "reparto_id": row.reparto_id,
        "reparto_codice": row.reparto.Codice if row.reparto else "",
        "reparto_descrizione": row.reparto.Descrizione if row.reparto else "",
        "role_id": row.role_id,
        "role_name": row.role.name if row.role else "",
        "role_description": row.role.description if row.role else "",
        "user_id": row.user_id,
        "username": row.user.username if row.user else "",
        "apply_to": row.apply_to or "macchine",
        "phase_mode": row.phase_mode or "all",
        "phase_values": row.phase_values or "",
        "attivo": bool(row.attivo),
        "titolo_macchine_da_eseguire": row.titolo_macchine_da_eseguire or "",
        "titolo_macchine_attive": row.titolo_macchine_attive or "",
        "testo_presa_macchina": row.testo_presa_macchina or "",
        "testo_sospendi_macchina": row.testo_sospendi_macchina or "",
        "testo_riattiva_macchina": row.testo_riattiva_macchina or "",
        "testo_chiudi_macchina": row.testo_chiudi_macchina or "",
        "metodo_documentale_tipo": row.metodo_documentale_tipo or "",
        "metodo_documentale_prefisso": row.metodo_documentale_prefisso or "",
        "metodo_documentale_path_key": row.metodo_documentale_path_key or "",
    }


def _home_config_manageable_users(policy: RbacPolicy) -> list[User]:
    manageable_role_ids = set(policy.descendant_manageable_role_ids)

    if not manageable_role_ids:
        return []

    ur_allowed = user_roles.alias("home_cfg_ur_allowed")
    ur_forbidden = user_roles.alias("home_cfg_ur_forbidden")

    allowed_exists = exists(
        select(1)
        .select_from(ur_allowed)
        .where(
            and_(
                ur_allowed.c.user_id == User.id,
                ur_allowed.c.role_id.in_(manageable_role_ids),
            )
        )
    )

    forbidden_exists = exists(
        select(1)
        .select_from(ur_forbidden)
        .where(
            and_(
                ur_forbidden.c.user_id == User.id,
                ~ur_forbidden.c.role_id.in_(manageable_role_ids),
            )
        )
    )

    return (
        User.query.filter(User.active.is_(True))
        .filter(User.id != _current_user_id())
        .filter(allowed_exists)
        .filter(~forbidden_exists)
        .order_by(func.lower(User.username))
        .all()
    )


def _home_config_user_is_manageable(policy: RbacPolicy, user: User | None) -> bool:
    if user is None:
        return False

    manageable_role_ids = set(policy.descendant_manageable_role_ids)
    if not manageable_role_ids:
        return False

    target_roles = list(user.roles or [])
    if not target_roles:
        return False

    return all(int(role.id) in manageable_role_ids for role in target_roles)


def _home_config_role_is_manageable(policy: RbacPolicy, role: Roles | None) -> bool:
    if role is None:
        return False

    return int(role.id) in set(policy.descendant_manageable_role_ids)


def _build_home_config_settings_payload(policy: RbacPolicy) -> dict:
    reparto_rows = Reparti.query.order_by(
        func.lower(func.coalesce(Reparti.Descrizione, Reparti.Codice)),
        func.lower(Reparti.Codice),
    ).all()

    config_rows = (
        HomeRepartoConfig.query.options(selectinload(HomeRepartoConfig.reparto))
        .order_by(HomeRepartoConfig.ordine_menu.asc(), HomeRepartoConfig.id.asc())
        .all()
    )

    rule_rows = (
        HomeVisibilityRule.query.options(
            selectinload(HomeVisibilityRule.reparto),
            selectinload(HomeVisibilityRule.role),
            selectinload(HomeVisibilityRule.user),
        )
        .order_by(HomeVisibilityRule.reparto_id.asc(), HomeVisibilityRule.id.asc())
        .all()
    )

    manageable_roles = sorted(
        list(policy.descendant_manageable_roles),
        key=lambda r: ((r.description or r.name or "").lower(), (r.name or "").lower()),
    )

    manageable_users = _home_config_manageable_users(policy)

    return {
        "reparti": [
            {
                "id": r.id,
                "codice": r.Codice or "",
                "descrizione": r.Descrizione or r.Codice or "",
            }
            for r in reparto_rows
        ],
        "roles": [
            {
                "id": r.id,
                "name": r.name or "",
                "description": r.description or r.name or "",
            }
            for r in manageable_roles
        ],
        "users": [
            {
                "id": u.id,
                "username": u.username or "",
            }
            for u in manageable_users
        ],
        "home_configs": [_home_reparto_config_to_dict(row) for row in config_rows],
        "visibility_rules": [_home_visibility_rule_to_dict(row) for row in rule_rows],
        "template_options": HOME_CONFIG_TEMPLATE_OPTIONS,
        "renderer_options": HOME_CONFIG_RENDERER_OPTIONS,
        "metodo_options": HOME_CONFIG_METODO_OPTIONS,
        "apply_to_options": HOME_RULE_APPLY_TO_OPTIONS,
        "phase_mode_options": HOME_RULE_PHASE_MODE_OPTIONS,
    }


def _parse_home_rule_phase_values(raw_values, phase_mode: str) -> str | None:
    phase_mode = _home_config_text(phase_mode).lower()

    if phase_mode in {"all", "last", "not_first"}:
        return None

    if isinstance(raw_values, str):
        raw_values = raw_values.strip()

        if raw_values.startswith("["):
            try:
                parsed = json.loads(raw_values)
            except json.JSONDecodeError:
                parsed = [raw_values]
        else:
            parsed = [x.strip() for x in raw_values.split(",")]
    elif isinstance(raw_values, (list, tuple, set)):
        parsed = list(raw_values)
    else:
        parsed = []

    values = []
    for item in parsed:
        value = _home_config_text(item)
        if not value:
            continue

        try:
            phase_int = int(float(value))
            if phase_int <= 0:
                raise ValueError
            value = str(phase_int)
        except (TypeError, ValueError):
            raise ValueError("I valori fase devono essere numerici.")

        if value not in values:
            values.append(value)

    if phase_mode in {"exact", "list"} and not values:
        raise ValueError("Per fase esatta/lista devi indicare almeno un valore fase.")

    return json.dumps(values, ensure_ascii=False)


def _capacity_calendar_payload() -> list[dict]:
    rows = (
        ProductionCapacityCalendar.query.filter(
            ProductionCapacityCalendar.active.is_(True)
        )
        .order_by(
            ProductionCapacityCalendar.scope_type.asc(),
            ProductionCapacityCalendar.scope_code.asc(),
            ProductionCapacityCalendar.weekday.asc(),
        )
        .all()
    )

    return [
        {
            "id": row.id,
            "scope_type": row.scope_type,
            "scope_code": row.scope_code,
            "weekday": row.weekday,
            "hours_capacity": float(row.hours_capacity or 0),
        }
        for row in rows
    ]


def _safe_filename(value: str) -> str:
    value = str(value or "").strip()
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    return value or "etichetta"


def _genera_e_salva_etichetta_lotto(
    *,
    codice: str,
    descrizione: str,
    lotto: str,
    quantita: str,
) -> str:
    """
    Genera il PNG dell'etichetta lotto e restituisce il nome file salvato.
    """
    img = gen_etichette(
        codice=codice,
        descrizione=descrizione,
        lotto=lotto,
        qty=quantita,
        label_dimensions=current_app.config["DIMENSIONI"],
        dpi=current_app.config["DPI"],
        font_path=current_app.config["FONT_PATH"],
    )

    output_dir = Path(current_app.config["ETICHETTE_OUTPUT_DIR"])
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _now_rome_dt().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid4().hex[:8]
    filename = f"etichetta_{_safe_filename(lotto)}_{timestamp}_{unique_suffix}.png"
    file_path = output_dir / filename

    img.save(file_path, format="PNG")

    return filename


def _resolve_label_file_path(filename: str) -> Path | None:
    filename = _norm_text(filename)
    if not filename:
        return None

    base_dir = Path(current_app.config["ETICHETTE_OUTPUT_DIR"]).expanduser()

    try:
        base_dir = base_dir.resolve()
        file_path = (base_dir / filename).resolve()
        file_path.relative_to(base_dir)
    except Exception:
        return None

    return file_path


def _apply_label_image_offset(img, offset_x_mm: float, offset_y_mm: float, dpi: int):
    """
    Applica offset al PNG mantenendo invariata la dimensione finale.

    Offset positivo X = sposta a destra.
    Offset negativo X = sposta a sinistra.
    Offset positivo Y = sposta in basso.
    Offset negativo Y = sposta in alto.
    """
    offset_x_px = int(round(float(offset_x_mm) / 25.4 * int(dpi)))
    offset_y_px = int(round(float(offset_y_mm) / 25.4 * int(dpi)))

    if offset_x_px == 0 and offset_y_px == 0:
        return img

    canvas = Image.new("RGB", img.size, "white")

    src_left = max(0, -offset_x_px)
    src_top = max(0, -offset_y_px)
    src_right = (
        min(img.width, img.width - offset_x_px) if offset_x_px > 0 else img.width
    )
    src_bottom = (
        min(img.height, img.height - offset_y_px) if offset_y_px > 0 else img.height
    )

    dst_left = max(0, offset_x_px)
    dst_top = max(0, offset_y_px)

    if src_right <= src_left or src_bottom <= src_top:
        return img

    cropped = img.crop((src_left, src_top, src_right, src_bottom))
    canvas.paste(cropped, (dst_left, dst_top))

    return canvas


def _mm_to_printer_px(mm: float, dpi: int) -> int:
    return int(round(float(mm) / 25.4 * int(dpi)))


def _get_label_print_settings() -> dict:
    dimensioni = current_app.config.get("DIMENSIONI") or [80.0, 50.0]

    return {
        "printer_name": current_app.config.get("LABEL_PRINTER_NAME") or "",
        "width_mm": float(dimensioni[0]),
        "height_mm": float(dimensioni[1]),
        "dpi": int(current_app.config.get("DPI") or 300),
        "rotation": int(current_app.config.get("LABEL_PRINT_ROTATION", 0) or 0),
        "offset_x_mm": float(
            current_app.config.get("LABEL_PRINT_OFFSET_X_MM", 0.0) or 0.0
        ),
        "offset_y_mm": float(
            current_app.config.get("LABEL_PRINT_OFFSET_Y_MM", 0.0) or 0.0
        ),
        "scale": float(current_app.config.get("LABEL_PRINT_SCALE", 1.0) or 1.0),
    }


def _create_label_printer_dc(printer_name: str, width_mm: float, height_mm: float):
    """
    Crea il Device Context della stampante etichette.

    Non forza PaperWidth/PaperLength da Python perché alcuni ambienti pywin32
    espongono win32gui.CreateDC con soli 3 argomenti.
    Il formato 80x50 deve essere configurato nel driver Windows della CAB.
    """
    printer_dc = win32ui.CreateDC()
    printer_dc.CreatePrinterDC(printer_name)
    return printer_dc


def _print_label_png_to_windows_printer(file_path: Path) -> None:
    settings = _get_label_print_settings()

    printer_name = settings["printer_name"]
    if not printer_name:
        raise RuntimeError("Nome stampante etichette non configurato.")

    width_mm = settings["width_mm"]
    height_mm = settings["height_mm"]
    dpi = settings["dpi"]
    rotation = settings["rotation"]
    offset_x_mm = settings["offset_x_mm"]
    offset_y_mm = settings["offset_y_mm"]
    scale = settings["scale"]

    if not file_path or not Path(file_path).is_file():
        raise FileNotFoundError(f"File etichetta non trovato: {file_path}")

    img = Image.open(file_path)
    img = ImageOps.exif_transpose(img).convert("RGB")
    img = _apply_label_image_offset(
        img,
        offset_x_mm=offset_x_mm,
        offset_y_mm=offset_y_mm,
        dpi=dpi,
    )

    if rotation:
        # PIL ruota in senso antiorario.
        img = img.rotate(rotation, expand=True)

    # Dimensione fisica voluta: 80x50 mm a 300 dpi.
    target_w_px = _mm_to_printer_px(width_mm * scale, dpi)
    target_h_px = _mm_to_printer_px(height_mm * scale, dpi)

    offset_x_px = _mm_to_printer_px(offset_x_mm, dpi)
    offset_y_px = _mm_to_printer_px(offset_y_mm, dpi)

    printer_dc = _create_label_printer_dc(printer_name, width_mm, height_mm)

    started_doc = False
    started_page = False

    try:
        printable_w = printer_dc.GetDeviceCaps(win32con.HORZRES)
        printable_h = printer_dc.GetDeviceCaps(win32con.VERTRES)
        expected_w = target_w_px
        expected_h = target_h_px

        max_w = int(expected_w * 1.35)
        max_h = int(expected_h * 1.35)

        if printable_w > max_w or printable_h > max_h:
            raise RuntimeError(
                "Formato pagina driver non coerente con etichetta. "
                f"Atteso circa {expected_w}x{expected_h}px, "
                f"driver restituisce {printable_w}x{printable_h}px. "
                "Configura nel driver Windows della cab EOS1/300 un formato 80x50 mm."
            )

        current_app.logger.info(
            "Stampa etichetta: file=%s printer=%s img=%sx%s target=%sx%s printable=%sx%s dpi=%s rotation=%s offset=%s,%s",
            file_path,
            printer_name,
            img.width,
            img.height,
            target_w_px,
            target_h_px,
            printable_w,
            printable_h,
            dpi,
            rotation,
            offset_x_px,
            offset_y_px,
        )

        # Se il driver restituisce un'area stampabile leggermente diversa,
        # evitiamo di uscire dal formato etichetta.
        draw_w = min(target_w_px, printable_w)
        draw_h = min(target_h_px, printable_h)

        x1 = 0
        y1 = 0
        x2 = draw_w
        y2 = draw_h

        dib = ImageWin.Dib(img)

        printer_dc.StartDoc(str(file_path.name))
        started_doc = True

        printer_dc.StartPage()
        started_page = True

        # Stampa una singola immagine in una singola area 80x50.
        dib.draw(printer_dc.GetHandleOutput(), (x1, y1, x2, y2))

        printer_dc.EndPage()
        started_page = False

        printer_dc.EndDoc()
        started_doc = False

    except Exception:
        if started_page:
            try:
                printer_dc.EndPage()
            except Exception:
                pass

        if started_doc:
            try:
                printer_dc.AbortDoc()
            except Exception:
                pass

        raise

    finally:
        printer_dc.DeleteDC()


def _priority_sort_key(ordine):
    pr = getattr(ordine, "PrioritaNumero", None)
    pos = getattr(ordine, "PrioritaPosizione", None)

    if pr in (1, 2, 3):
        return (
            0,
            pr,
            pos or 999999,
            _norm_text(ordine.DataFineSched),
            _norm_text(ordine.RifRegistraz),
        )

    return (
        1,
        99,
        999999,
        _norm_text(ordine.DataFineSched),
        _norm_text(ordine.RifRegistraz),
    )


def _apply_priorita_to_ordini(
    ordini: list[InputOdp],
    operatore_id: int,
    *,
    sort_result: bool = True,
) -> list[InputOdp]:
    priorita_map = _priorita_map_for_operatore(operatore_id)

    for ordine in ordini:
        row = priorita_map.get(_ordine_fase_key(ordine))

        if row is None:
            ordine.PrioritaNumero = None
            ordine.PrioritaPosizione = None
        else:
            ordine.PrioritaNumero = row.Priorita
            ordine.PrioritaPosizione = row.Posizione

    if sort_result:
        return sorted(ordini, key=_priority_sort_key)

    return ordini


def _make_ordine_fase_key(id_documento, id_riga, fase) -> tuple[str, str, str]:
    return (
        _norm_text(id_documento),
        _norm_text(id_riga),
        _norm_text(fase) or "1",
    )


def _priority_now_iso() -> str:
    return datetime.now(ROME_TZ).isoformat(timespec="seconds")


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


def _stato_operativo_chiusura(ordine, stato=None) -> str:
    """
    Stato reale da usare per decidere se un ordine è chiudibile.

    Priorità:
    1. runtime.Stato_odp
    2. ordine.StatoOrdine
    """
    stato_runtime = _norm_text(getattr(stato, "Stato_odp", ""))
    if stato_runtime:
        return stato_runtime

    return _norm_text(getattr(ordine, "StatoOrdine", ""))


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


from app_odp.routes_modules import (
    acquisti,
    priorita,
    dashboard,
    etichette,
    impostazioni,
    ordini,
    erp,
    documenti,
    home,
    report_settimanale,
)
